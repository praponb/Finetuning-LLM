WITH filtered_students AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international
    FROM students
    WHERE is_international = TRUE
      AND cohort = 'C2021'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        major,
        gpa,
        is_international,
        SUM(gpa) OVER (ORDER BY enrolled_on, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS gpa_sum_1_prior,
        COUNT(*) OVER (PARTITION BY major, is_international) AS rows_in_major_is_international,
        ROUND(gpa / NULLIF(SUM(gpa) OVER (PARTITION BY MONTH(enrolled_on)), 0), 4) AS gpa_share_of_enrolled_on_month,
        COUNT(*) OVER (ORDER BY enrolled_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        DENSE_RANK() OVER w_all_by_enrolled_on_ties AS enrolled_on_dense_rank,
        FIRST_VALUE(gpa) OVER (PARTITION BY is_international ORDER BY enrolled_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_gpa,
        NTILE(10) OVER (PARTITION BY is_international ORDER BY enrolled_on, id) AS enrolled_on_decile,
        SUM(gpa) OVER (PARTITION BY major, cohort ORDER BY gpa DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_gpa,
        MAX(gpa) OVER w_enrolled_on_month AS enrolled_on_month_max_gpa
    FROM filtered_students
    WINDOW w_all_by_enrolled_on_ties AS (ORDER BY enrolled_on), w_enrolled_on_month AS (PARTITION BY MONTH(enrolled_on))
),
totals AS (
    -- Group-level totals
    SELECT
        enrolled_on_dense_rank,
        COUNT(DISTINCT major) AS distinct_majors,
        SUM(rows_in_major_is_international) AS total_rows_in_major_is_international,
        ROUND(SUM(enrolled_on_month_max_gpa), 2) AS total_enrolled_on_month_max_gpa
    FROM windowed
    GROUP BY enrolled_on_dense_rank
    HAVING MAX(enrolled_on_decile) > 3
)
SELECT
    enrolled_on_dense_rank,
    distinct_majors,
    FLOOR(enrolled_on_dense_rank / 2) * 2 AS enrolled_on_dense_rank_bucket,
    ROUND(distinct_majors / NULLIF(enrolled_on_dense_rank, 0), 3) AS distinct_majors_per_enrolled_on_dense_rank
FROM totals
