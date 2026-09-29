WITH recent_courses AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        course_code,
        dept,
        level,
        CASE WHEN dept IN ('Bio', 'Math') THEN 'group_a' WHEN dept = 'Biz' THEN 'group_b' ELSE 'other' END AS dept_group
    FROM courses
    WHERE UPPER(TRIM(course_code)) LIKE 'A%'
      AND credits BETWEEN 2 AND 4
)
SELECT id
FROM recent_courses
INTERSECT ALL
SELECT student_id
FROM enrollments
WHERE course_id <= 8
  AND grade_status IN ('FAIL', 'WITHDRAWN')
