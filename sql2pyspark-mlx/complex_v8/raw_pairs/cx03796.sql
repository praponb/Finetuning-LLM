WITH cleaned_enrollments AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        student_id,
        course_id,
        term,
        score,
        grade_status,
        enrolled_at
    FROM enrollments
    WHERE score BETWEEN 26.2 AND 85.8
)
SELECT grade_status AS name, score, 'enrollment' AS origin
FROM cleaned_enrollments
UNION ALL
SELECT cohort AS name, gpa AS score, 'student' AS origin
FROM students
WHERE UPPER(TRIM(student_name)) LIKE 'H%'
UNION ALL
SELECT term AS name, score, 'enrollment_3' AS origin
FROM enrollments
ORDER BY origin, score
