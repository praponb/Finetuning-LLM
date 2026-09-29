WITH valid_enrollments AS (
    -- Scope the enrollments rows
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        COALESCE(score, 0.0) AS score_filled,
        LENGTH(TRIM(term)) AS term_len
    FROM enrollments
    WHERE score > 32.9
      AND course_id >= 9
),
latest AS (
    -- Keep the latest row per course_id
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at,
        score_filled,
        term_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY course_id ORDER BY score DESC, id DESC) AS recency_rank
        FROM valid_enrollments
    ) ranked
    WHERE recency_rank = 1
),
stats AS (
    -- Summarise per group
    SELECT
        term,
        MIN(term_len) AS min_term_len,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT grade_status) AS distinct_grade_status
    FROM latest
    GROUP BY term
    HAVING SUM(score_filled) > 30.0
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM stats
    WHERE n_rows < (SELECT MAX(n_rows) FROM stats)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(min_term_len) OVER (PARTITION BY min_term_len, term ORDER BY n_rows ASC NULLS LAST, term ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS min_term_len_sum_4,
        ROUND(CUME_DIST() OVER (PARTITION BY min_term_len, term ORDER BY n_rows ASC NULLS LAST), 3) AS n_rows_cume_dist,
        min_term_len - LAG(min_term_len) OVER w_n_rows_by_min_term_len AS min_term_len_change
    FROM qualified
    WINDOW w_n_rows_by_min_term_len AS (PARTITION BY n_rows ORDER BY min_term_len ASC, term)
),
annotated AS (
    -- Derive helper columns
    SELECT
        term,
        min_term_len,
        distinct_grade_status,
        min_term_len_sum_4,
        n_rows_cume_dist,
        CONCAT(term, '#', CAST(n_rows AS STRING)) AS term_ref
    FROM metrics
),
metrics_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(n_rows_cume_dist) OVER (ORDER BY min_term_len DESC, term ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS n_rows_cume_dist_avg_6_prior,
        n_rows_cume_dist - LAG(n_rows_cume_dist) OVER (ORDER BY min_term_len DESC, term) AS n_rows_cume_dist_change,
        DENSE_RANK() OVER (ORDER BY min_term_len DESC, term) AS min_term_len_dense_rank
    FROM annotated
)
SELECT term, metric, metric_value
FROM (
    SELECT term, min_term_len, distinct_grade_status
    FROM metrics_2
) src
UNPIVOT (
    metric_value FOR metric IN (min_term_len, distinct_grade_status)
) unp
