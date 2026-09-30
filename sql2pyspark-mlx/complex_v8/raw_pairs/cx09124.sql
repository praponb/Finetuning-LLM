WITH recent_students AS (
    -- Scope the students rows
    SELECT
        id,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        INITCAP(TRIM(student_name)) AS student_name_clean
    FROM students
    WHERE UPPER(TRIM(student_name)) LIKE 'M%'
      AND major IN ('Biz', 'Math', 'Art')
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        cohort,
        enrolled_on,
        gpa,
        is_international,
        student_name_clean,
        LAG(enrolled_on, 3) OVER w_all_by_gpa AS prev3_enrolled_on,
        SIZE(COLLECT_SET(cohort) OVER (PARTITION BY major, is_international)) AS distinct_cohort_in_major_is_international,
        ROUND(gpa / NULLIF(SUM(gpa) OVER w_major_is_international, 0), 4) AS gpa_share_of_major_is_international,
        ROUND(gpa - AVG(gpa) OVER (PARTITION BY major, is_international), 2) AS gpa_vs_major_is_international_avg,
        COUNT(*) OVER (PARTITION BY major, is_international ORDER BY enrolled_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LAG(enrolled_on) OVER w_major_is_international_by_en_2 AS prev_enrolled_on,
        DENSE_RANK() OVER (ORDER BY gpa DESC, id) AS gpa_dense_rank,
        ROW_NUMBER() OVER w_all_by_gpa AS gpa_seq,
        ROUND(CUME_DIST() OVER (ORDER BY gpa DESC), 3) AS gpa_cume_dist,
        ROUND(PERCENT_RANK() OVER (ORDER BY gpa DESC, id), 3) AS gpa_percent_rank,
        MAX(gpa) OVER (PARTITION BY major, is_international ORDER BY enrolled_on DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS gpa_max_6_prior,
        SUM(gpa) OVER (ORDER BY gpa DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_gpa,
        NTH_VALUE(enrolled_on, 2) OVER (ORDER BY gpa DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_enrolled_on
    FROM recent_students
    WINDOW w_all_by_gpa AS (ORDER BY gpa DESC, id), w_major_is_international AS (PARTITION BY major, is_international), w_major_is_international_by_en_2 AS (PARTITION BY major, is_international ORDER BY enrolled_on DESC, id)
),
top_rows AS (
    -- Top 3 per group
    SELECT *
    FROM metrics
    WHERE gpa_dense_rank <= 3
)
-- Final projection
SELECT
    id,
    cohort,
    enrolled_on,
    gpa,
    is_international,
    student_name_clean,
    prev3_enrolled_on,
    distinct_cohort_in_major_is_international,
    gpa_vs_major_is_international_avg,
    running_count,
    prev_enrolled_on,
    gpa_seq,
    gpa_cume_dist,
    gpa_percent_rank,
    gpa_max_6_prior,
    running_gpa
FROM top_rows
