WITH stacked AS (
    -- Stack the branches
    SELECT id AS qty, enrolled_on AS event_date
    FROM students
    WHERE cohort = 'C2024'
      AND enrolled_on >= DATE '2024-05-18'
    UNION ALL
    SELECT id, enrolled_on
    FROM students
    WHERE NOT is_international
      AND cohort IN ('C2022', 'C2024')
)
SELECT qty, COUNT(*) AS n_rows, MAX(event_date) AS latest_event_date
FROM stacked
GROUP BY qty
ORDER BY qty
