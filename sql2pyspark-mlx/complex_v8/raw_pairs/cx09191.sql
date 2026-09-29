WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        cohort,
        major,
        gpa
    FROM students
    WHERE gpa BETWEEN 2.5 AND 3.9
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        student_name,
        cohort,
        major,
        LAST_VALUE(gpa) OVER (PARTITION BY major ORDER BY gpa ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_gpa,
        LEAD(gpa, 1, 0) OVER w_all_by_gpa AS next_gpa,
        ROUND(gpa - AVG(gpa) OVER (PARTITION BY major, cohort), 2) AS gpa_vs_major_cohort_avg,
        NTH_VALUE(gpa, 2) OVER (ORDER BY gpa, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_gpa,
        SUM(gpa) OVER (PARTITION BY cohort) AS cohort_sum_gpa,
        ROUND(AVG(gpa) OVER (PARTITION BY major, cohort), 2) AS major_cohort_avg_gpa,
        FIRST_VALUE(gpa) OVER (ORDER BY gpa, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_gpa,
        NTH_VALUE(gpa, 2) OVER (PARTITION BY cohort, major ORDER BY gpa DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_gpa_2,
        NTH_VALUE(gpa, 2) OVER (PARTITION BY cohort, major ORDER BY gpa, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_gpa_3,
        gpa - LAG(gpa) OVER (ORDER BY gpa ASC, id) AS gpa_change,
        MAX(gpa) OVER (PARTITION BY major, cohort) AS major_cohort_max_gpa,
        ROUND(PERCENT_RANK() OVER (PARTITION BY cohort, major ORDER BY gpa, id), 3) AS gpa_percent_rank,
        SUM(gpa) OVER (PARTITION BY major ORDER BY gpa ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_gpa,
        SUM(gpa) OVER (PARTITION BY major, cohort) AS major_cohort_sum_gpa,
        MAX(gpa) OVER (ORDER BY gpa, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS gpa_max_2_prior
    FROM recent
    WINDOW w_all_by_gpa AS (ORDER BY gpa, id)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        student_name,
        cohort,
        major,
        last_gpa,
        next_gpa,
        second_gpa,
        major_cohort_avg_gpa,
        first_gpa,
        second_gpa_2,
        gpa_change,
        major_cohort_max_gpa,
        gpa_percent_rank,
        major_cohort_sum_gpa,
        gpa_max_2_prior,
        ROW_NUMBER() OVER (PARTITION BY cohort ORDER BY cohort_sum_gpa, id) AS seq_no,
        SUM(gpa_percent_rank) OVER (PARTITION BY major) AS major_sum_gpa_percent_rank,
        COUNT(*) OVER (PARTITION BY major, cohort) AS rows_in_major_cohort,
        ROUND(last_gpa - AVG(last_gpa) OVER (PARTITION BY major, cohort), 2) AS last_gpa_vs_major_cohort_avg,
        MIN(last_gpa) OVER w_major AS major_min_last_gpa
    FROM sequenced
    WINDOW w_major AS (PARTITION BY major)
)
-- Final projection
SELECT
    id,
    student_name,
    cohort,
    major,
    last_gpa,
    next_gpa,
    major_cohort_avg_gpa,
    gpa_change,
    major_cohort_max_gpa,
    gpa_percent_rank,
    major_cohort_sum_gpa,
    gpa_max_2_prior,
    rows_in_major_cohort,
    last_gpa_vs_major_cohort_avg,
    major_min_last_gpa,
    LENGTH(TRIM(major)) AS major_len,
    GREATEST(second_gpa_2, major_cohort_max_gpa) AS greatest_second_gpa_2_major_cohort_max_gpa
FROM metrics
WHERE major_cohort_max_gpa >= 15.0
ORDER BY major_len DESC
