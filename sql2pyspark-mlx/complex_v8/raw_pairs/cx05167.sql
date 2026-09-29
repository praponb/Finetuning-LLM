WITH combined AS (
    -- Stack the branches
    SELECT dept AS label, id AS ref_id, level AS category
    FROM courses
    WHERE LENGTH(TRIM(course_code)) >= 5
      AND dept IN ('Bio', 'Art', 'Math')
    UNION
    SELECT course_code AS label, credits AS ref_id, dept AS category
    FROM courses
    WHERE LENGTH(TRIM(course_code)) >= 6
)
SELECT label, COUNT(*) AS n_rows, SUM(ref_id) AS total_ref_id
FROM combined
GROUP BY label
ORDER BY label
