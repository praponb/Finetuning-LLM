WITH filtered_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        FLOOR(score / 25) * 25 AS score_bucket,
        LOWER(REGEXP_REPLACE(TRIM(term), ' ', '_')) AS term_slug,
        COALESCE(score, 0.0) AS score_filled
    FROM enrollments
    WHERE term = '2025T1'
      AND grade_status = 'WITHDRAWN'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        term,
        grade_status,
        enrolled_at,
        score_bucket,
        term_slug,
        score_filled,
        ROUND(AVG(score_bucket) OVER (ORDER BY score_filled DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS score_bucket_avg_1_prior,
        ROUND(score_filled - AVG(score_filled) OVER (PARTITION BY term), 2) AS score_filled_vs_term_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY course_id, grade_status ORDER BY score_bucket NULLS LAST), 3) AS score_bucket_percent_rank,
        COUNT(*) OVER (PARTITION BY term) AS rows_in_term,
        NTILE(3) OVER (PARTITION BY course_id, grade_status ORDER BY score_bucket NULLS LAST, id) AS score_bucket_tercile
    FROM filtered_enrollments
    WINDOW w_term AS (PARTITION BY term)
),
term_stats AS (
    -- Avg rows_in_term per term
    SELECT term, AVG(rows_in_term) AS avg_rows_in_term
    FROM metrics
    GROUP BY term
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_rows_in_term,
        ROUND(s.rows_in_term - g.avg_rows_in_term, 2) AS rows_in_term_minus_avg
    FROM metrics s
    JOIN term_stats g ON g.term = s.term
    WHERE s.rows_in_term > g.avg_rows_in_term
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(rows_in_term_minus_avg, 2) OVER (PARTITION BY grade_status ORDER BY score_bucket_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_rows_in_term_minus_avg,
        MAX(score_bucket_avg_1_prior) OVER w_term AS term_max_score_bucket_avg_1_prior,
        MIN(score_bucket_tercile) OVER (PARTITION BY term) AS term_min_score_bucket_tercile
    FROM vs_group
    WINDOW w_term AS (PARTITION BY term)
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        grade_status,
        enrolled_at,
        score_filled,
        score_bucket_avg_1_prior,
        score_filled_vs_term_avg,
        score_bucket_percent_rank,
        rows_in_term,
        score_bucket_tercile,
        avg_rows_in_term,
        rows_in_term_minus_avg,
        second_rows_in_term_minus_avg,
        term_max_score_bucket_avg_1_prior,
        term_min_score_bucket_tercile,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd HH:00') AS enrolled_at_label,
        LEAST(rows_in_term, score_bucket_tercile) AS least_rows_in_term_score_bucket_tercile,
        GREATEST(rows_in_term_minus_avg, score_filled_vs_term_avg) AS greatest_rows_in_term_minus_avg_score_filled_vs_
    FROM sequenced
    WHERE grade_status IN ('WITHDRAWN', 'PASS')
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM annotated
    WHERE term_max_score_bucket_avg_1_prior > (SELECT AVG(term_max_score_bucket_avg_1_prior) FROM annotated)
)
SELECT
    id,
    grade_status,
    enrolled_at,
    score_filled,
    score_bucket_avg_1_prior,
    score_bucket_percent_rank,
    score_bucket_tercile,
    avg_rows_in_term,
    rows_in_term_minus_avg,
    term_max_score_bucket_avg_1_prior,
    least_rows_in_term_score_bucket_tercile,
    greatest_rows_in_term_minus_avg_score_filled_vs_
FROM filtered
ORDER BY greatest_rows_in_term_minus_avg_score_filled_vs_ DESC, avg_rows_in_term
