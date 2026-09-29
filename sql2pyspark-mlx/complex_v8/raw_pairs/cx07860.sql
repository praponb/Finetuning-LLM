WITH valid_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        enrolled_at
    FROM enrollments
    WHERE course_id <= 5
      AND term IN ('2025T1', '2024T1')
),
with_activity AS (
    -- Rows with no matching enrollments
    SELECT s.*
    FROM valid_enrollments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM enrollments e
            WHERE e.course_id = s.course_id
              AND e.student_id IS NOT NULL
              AND e.score >= 8.7
        )
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        course_id,
        term,
        enrolled_at,
        DATE_TRUNC('YEAR', enrolled_at) AS enrolled_at_year,
        FLOOR(score / 25) * 25 AS score_bucket
    FROM with_activity
    WHERE course_id >= 4
),
summary AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(enrolled_at, 'yyyy-MM') AS enrolled_at_ym,
        YEAR(enrolled_at_year) AS enrolled_at_year_year,
        COUNT(*) AS cnt,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT term) AS distinct_terms,
        MIN(score_bucket) AS min_score_bucket
    FROM annotated
    GROUP BY DATE_FORMAT(enrolled_at, 'yyyy-MM'), YEAR(enrolled_at_year)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        enrolled_at_ym,
        enrolled_at_year_year,
        cnt,
        distinct_ids,
        min_score_bucket,
        ROUND(AVG(enrolled_at_year_year) OVER (PARTITION BY enrolled_at_year_year, distinct_ids ORDER BY distinct_ids NULLS LAST, enrolled_at_ym, enrolled_at_year_year ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS enrolled_at_year_year_avg_2_prior,
        MAX(distinct_terms) OVER w_enrolled_at_year_year AS enrolled_at_year_year_max_distinct_terms,
        ROW_NUMBER() OVER w_enrolled_at_year_year_distin_2 AS rn
    FROM summary
    WINDOW w_enrolled_at_year_year AS (PARTITION BY enrolled_at_year_year), w_enrolled_at_year_year_distin_2 AS (PARTITION BY enrolled_at_year_year, distinct_ids ORDER BY distinct_ids NULLS LAST, enrolled_at_ym, enrolled_at_year_year)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        enrolled_at_ym,
        enrolled_at_year_year,
        cnt,
        distinct_ids,
        min_score_bucket,
        enrolled_at_year_year_avg_2_prior,
        enrolled_at_year_year_max_distinct_terms,
        rn,
        DENSE_RANK() OVER (ORDER BY rn DESC) AS rn_dense_rank,
        LAG(enrolled_at_year_year) OVER (ORDER BY rn DESC, enrolled_at_ym, enrolled_at_year_year) AS prev_enrolled_at_year_year,
        COUNT(*) OVER (ORDER BY rn DESC, enrolled_at_ym, enrolled_at_year_year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        DENSE_RANK() OVER (ORDER BY rn DESC, enrolled_at_ym, enrolled_at_year_year) AS rn_dense_rank_2,
        RANK() OVER (ORDER BY rn DESC, enrolled_at_ym, enrolled_at_year_year) AS rn_rank,
        NTILE(5) OVER (ORDER BY rn DESC, enrolled_at_ym, enrolled_at_year_year) AS rn_quintile
    FROM windowed
)
SELECT
    enrolled_at_ym,
    enrolled_at_year_year,
    cnt,
    min_score_bucket,
    enrolled_at_year_year_max_distinct_terms,
    rn,
    rn_dense_rank,
    prev_enrolled_at_year_year,
    running_count,
    rn_dense_rank_2,
    rn_rank,
    COALESCE(rn_dense_rank, 0) AS rn_dense_rank_filled
FROM metrics
ORDER BY enrolled_at_ym
