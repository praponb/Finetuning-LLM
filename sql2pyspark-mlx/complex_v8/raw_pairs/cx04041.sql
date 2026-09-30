WITH prepared AS (
    -- Scope the students rows
    SELECT
        id,
        student_name,
        cohort,
        major,
        enrolled_on,
        gpa
    FROM students
    WHERE gpa BETWEEN 2.0 AND 3.4
)
SELECT cohort AS name, major AS name_2, 'student' AS src
FROM prepared
WHERE YEAR(enrolled_on) = 2025
UNION ALL
SELECT course_code AS name, dept AS name_2, 'course' AS src
FROM courses
WHERE credits >= 2
  AND dept IN ('CS', 'Math', 'Art')
