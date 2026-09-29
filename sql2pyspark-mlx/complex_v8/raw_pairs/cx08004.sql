WITH filtered_students AS (
    -- Filter students
    SELECT
        id,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international
    FROM students
    WHERE major = 'Biz'
      AND UPPER(TRIM(student_name)) LIKE 'H%'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(gpa) OVER (ORDER BY enrolled_on DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_gpa,
        LAG(enrolled_on, 2) OVER w_cohort_major_by_gpa AS prev2_enrolled_on,
        DENSE_RANK() OVER (ORDER BY enrolled_on DESC, id) AS enrolled_on_dense_rank,
        LEAD(gpa) OVER (ORDER BY enrolled_on DESC, id) AS next_gpa,
        MIN(gpa) OVER (PARTITION BY cohort, major ORDER BY gpa DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_gpa_so_far,
        COUNT(*) OVER (PARTITION BY major, is_international) AS rows_in_major_is_international,
        LAG(gpa) OVER (PARTITION BY cohort, major ORDER BY gpa DESC, id) AS prev_gpa,
        ROUND(gpa - AVG(gpa) OVER w_is_international_enrolled_on, 2) AS gpa_vs_is_international_enrolled_on_year_avg,
        MAX(gpa) OVER (ORDER BY gpa ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS gpa_max_3_prior
    FROM filtered_students
    WINDOW w_cohort_major_by_gpa AS (PARTITION BY cohort, major ORDER BY gpa DESC, id), w_is_international_enrolled_on AS (PARTITION BY is_international, YEAR(enrolled_on))
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY major, enrolled_on_dense_rank) AS rows_in_major_enrolled_on_dense_rank,
        ROUND(last_gpa - AVG(last_gpa) OVER (PARTITION BY major, enrolled_on_dense_rank), 2) AS last_gpa_vs_major_enrolled_on_dense_rank_avg,
        MIN(next_gpa) OVER w_major_enrolled_on_dense_rank AS major_enrolled_on_dense_rank_min_next_gpa,
        FIRST_VALUE(min_gpa_so_far) OVER (PARTITION BY major, enrolled_on_dense_rank ORDER BY enrolled_on_dense_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_min_gpa_so_far,
        LEAD(enrolled_on_dense_rank, 1, 0) OVER (PARTITION BY major, is_international ORDER BY enrolled_on DESC, id) AS next_enrolled_on_dense_rank,
        LAG(gpa_max_3_prior) OVER w_major_is_international_by_en AS prev_gpa_max_3_prior
    FROM with_windows
    WINDOW w_major_enrolled_on_dense_rank AS (PARTITION BY major, enrolled_on_dense_rank), w_major_is_international_by_en AS (PARTITION BY major, is_international ORDER BY enrolled_on DESC, id)
),
totals AS (
    -- Summarise per group
    SELECT
        enrolled_on_dense_rank,
        DATE_TRUNC('MONTH', prev2_enrolled_on) AS prev2_enrolled_on_month,
        DATE_TRUNC('MONTH', enrolled_on) AS enrolled_on_month,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT id) AS distinct_ids
    FROM windowed
    GROUP BY enrolled_on_dense_rank, DATE_TRUNC('MONTH', prev2_enrolled_on), DATE_TRUNC('MONTH', enrolled_on)
    HAVING AVG(rows_in_major_enrolled_on_dense_rank) > 8
)
-- Final projection
SELECT
    enrolled_on_dense_rank,
    prev2_enrolled_on_month,
    enrolled_on_month,
    DATE_FORMAT(enrolled_on_month, 'yyyy-MM-dd') AS enrolled_on_month_label,
    distinct_ids - enrolled_on_dense_rank AS distinct_ids_minus_enrolled_on_dense_rank
FROM totals
WHERE prev2_enrolled_on_month >= TIMESTAMP '2025-01-17 09:00:00'
ORDER BY enrolled_on_dense_rank
