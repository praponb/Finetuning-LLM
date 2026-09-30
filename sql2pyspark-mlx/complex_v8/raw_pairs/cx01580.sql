WITH stacked AS (
    -- Stack the branches
    SELECT id AS ref_id, credits
    FROM courses
    WHERE dept = 'Art'
      AND UPPER(TRIM(course_code)) LIKE 'C%'
    UNION
    SELECT student_id AS ref_id, course_id AS credits
    FROM enrollments
    WHERE term <> '2024T1'
      AND student_id IS NOT NULL
)
SELECT ref_id, COUNT(*) AS n_rows, SUM(credits) AS total_credits
FROM stacked
GROUP BY ref_id
ORDER BY ref_id
