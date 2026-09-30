WITH cleaned AS (
    -- Filter enrollments
    SELECT *
    FROM enrollments
    WHERE student_id IS NOT NULL
      AND score < 79.2
)
SELECT course_id, grade_status AS name
FROM cleaned
WHERE term = '2025T2'
EXCEPT ALL
SELECT student_id, term
FROM enrollments
WHERE term = '2024T1'
ORDER BY name
