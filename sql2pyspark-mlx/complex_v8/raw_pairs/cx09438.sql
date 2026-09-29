WITH stacked AS (
    -- Stack the branches
    SELECT id AS ref_id, cohort AS category, enrolled_on
    FROM students
    UNION ALL
    SELECT id AS ref_id, cohort AS category, enrolled_on
    FROM students
    WHERE gpa BETWEEN 2.0 AND 3.4
)
SELECT category, COUNT(*) AS n_rows, SUM(ref_id) AS total_ref_id, MAX(enrolled_on) AS latest_enrolled_on
FROM stacked
GROUP BY category
ORDER BY category
