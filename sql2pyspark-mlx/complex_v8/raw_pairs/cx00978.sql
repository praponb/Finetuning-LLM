WITH scoped_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        grade_status,
        enrolled_at,
        DATE_FORMAT(enrolled_at, 'yyyy-MM-dd') AS enrolled_at_label
    FROM enrollments
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        course_id,
        grade_status,
        enrolled_at,
        enrolled_at_label,
        LAG(enrolled_at) OVER w_grade_status_by_enrolled_at AS prev_enrolled_at,
        NTH_VALUE(enrolled_at, 2) OVER (ORDER BY enrolled_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at,
        LAST_VALUE(enrolled_at) OVER (PARTITION BY grade_status ORDER BY enrolled_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_enrolled_at,
        NTH_VALUE(enrolled_at, 2) OVER (PARTITION BY course_id, term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_at_2,
        DENSE_RANK() OVER w_grade_status_by_enrolled_at_3 AS enrolled_at_dense_rank,
        LAG(enrolled_at) OVER (ORDER BY enrolled_at DESC, id) AS prev_enrolled_at_2,
        ROW_NUMBER() OVER (PARTITION BY grade_status ORDER BY enrolled_at DESC NULLS FIRST, id) AS rn,
        FIRST_VALUE(enrolled_at) IGNORE NULLS OVER (PARTITION BY course_id, term ORDER BY enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_enrolled_at,
        RANK() OVER (ORDER BY enrolled_at DESC, id) AS enrolled_at_rank
    FROM scoped_enrollments
    WINDOW w_grade_status_by_enrolled_at AS (PARTITION BY grade_status ORDER BY enrolled_at DESC NULLS FIRST, id), w_grade_status_by_enrolled_at_3 AS (PARTITION BY grade_status ORDER BY enrolled_at DESC NULLS FIRST)
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        enrolled_at_rank - rn AS enrolled_at_rank_minus_rn,
        CASE WHEN enrolled_at_dense_rank >= 6 THEN 'large' WHEN enrolled_at_dense_rank >= 5 THEN 'medium' ELSE 'small' END AS enrolled_at_dense_rank_tier,
        DATE_FORMAT(last_enrolled_at, 'MMM yyyy') AS last_enrolled_at_label
    FROM windowed
    WHERE enrolled_at_label IS NOT NULL
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        enrolled_at_dense_rank,
        YEAR(second_enrolled_at_2) AS second_enrolled_at_2_year,
        DATE_TRUNC('MONTH', prev_enrolled_at) AS prev_enrolled_at_month,
        COUNT(*) AS n_rows,
        ARRAY_SORT(COLLECT_SET(grade_status)) AS grade_status_list,
        ROUND(STDDEV(rn), 2) AS stddev_rn
    FROM labeled
    GROUP BY 1, 2, 3
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        enrolled_at_dense_rank - LAG(enrolled_at_dense_rank) OVER (ORDER BY n_rows, enrolled_at_dense_rank, second_enrolled_at_2_year, prev_enrolled_at_month) AS enrolled_at_dense_rank_change,
        SUM(n_rows) OVER (ORDER BY n_rows, enrolled_at_dense_rank, second_enrolled_at_2_year, prev_enrolled_at_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_n_rows
    FROM stats
)
-- Final projection
SELECT
    enrolled_at_dense_rank,
    second_enrolled_at_2_year,
    prev_enrolled_at_month,
    grade_status_list,
    stddev_rn,
    enrolled_at_dense_rank_change,
    running_n_rows,
    ROUND(stddev_rn * 1.07, 2) AS stddev_rn_adj,
    DATE_FORMAT(prev_enrolled_at_month, 'yyyy-MM-dd HH:00') AS prev_enrolled_at_month_label
FROM metrics
ORDER BY prev_enrolled_at_month
