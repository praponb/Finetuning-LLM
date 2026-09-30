WITH recent AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        CONCAT(major, '#', CAST(id AS STRING)) AS major_ref,
        SUBSTRING(TRIM(student_name), 1, 3) AS student_name_prefix
    FROM students
    WHERE major <> 'CS'
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.id) AS last_enrollment_enrolled_at
    FROM recent s
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(gpa) OVER (PARTITION BY cohort, is_international) AS cohort_is_international_sum_gpa,
        ROUND(AVG(gpa) OVER (PARTITION BY major ORDER BY last_enrollment_enrolled_at, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS gpa_avg_2_prior,
        SUM(gpa) OVER (PARTITION BY major ORDER BY last_enrollment_enrolled_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_gpa,
        ROUND(AVG(gpa) OVER (PARTITION BY cohort, is_international), 2) AS cohort_is_international_avg_gpa
    FROM with_stats
)
SELECT
    is_international,
    cohort,
    MIN(cohort_is_international_sum_gpa) AS min_cohort_is_international_sum_gpa
FROM with_windows
GROUP BY GROUPING SETS ((is_international, cohort), (is_international), (cohort), ())
