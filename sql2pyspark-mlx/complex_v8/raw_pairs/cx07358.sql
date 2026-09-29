WITH valid_students AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        CASE WHEN cohort IN ('C2021', 'C2022') THEN 'group_a' WHEN cohort = 'C2024' THEN 'group_b' ELSE 'other' END AS cohort_group
    FROM students
    WHERE major IS NOT NULL
),
summarized_rows AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MAX(e.enrolled_at) FROM enrollments e WHERE e.student_id = s.id) AS last_enrollment_enrolled_at,
        (SELECT COUNT(*) FROM enrollments e WHERE e.student_id = s.id) AS enrollment_count
    FROM valid_students s
)
SELECT
    is_international,
    enrollment_count,
    cohort,
    GROUPING_ID() AS grouping_level,
    COUNT(*) AS row_count,
    SUM(enrollment_count) AS total_enrollment_count,
    ROUND(100 * COUNT(CASE WHEN major IN ('Math', 'Art') THEN 1 END) / COUNT(*), 1) AS pct_major_in_math_art
FROM summarized_rows
GROUP BY GROUPING SETS ((is_international, enrollment_count, cohort), (is_international), (enrollment_count))
ORDER BY is_international NULLS LAST, enrollment_count NULLS LAST, cohort NULLS LAST
