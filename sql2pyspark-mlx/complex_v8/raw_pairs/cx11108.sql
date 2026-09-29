WITH valid_students AS (
    -- Filter students
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa,
        is_international,
        LOWER(TRIM(student_name)) AS student_name_clean
    FROM students
    WHERE UPPER(TRIM(student_name)) LIKE 'I%'
),
with_stats AS (
    -- Attach enrollments metrics
    SELECT
        s.*,
        (SELECT MIN(e.score) FROM enrollments e WHERE e.student_id = s.id) AS min_enrollment_score
    FROM valid_students s
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_stats
    WHERE gpa > (SELECT AVG(gpa) FROM with_stats)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(gpa) OVER (PARTITION BY cohort) AS cohort_min_gpa,
        ROUND(AVG(min_enrollment_score) OVER (PARTITION BY cohort), 2) AS cohort_avg_min_enrollment_score
    FROM filtered
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT major, cohort, cohort_min_gpa
    FROM with_trends
) src
PIVOT (
    COUNT(cohort_min_gpa)
    FOR cohort IN ('C2023', 'C2022')
)
ORDER BY major
