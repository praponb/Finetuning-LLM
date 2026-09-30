WITH combined AS (
    -- Stack the branches
    SELECT capacity, opened_on
    FROM warehouses
    WHERE UPPER(TRIM(wh_code)) LIKE 'C%'
      AND opened_on >= DATE '2024-11-25'
    UNION
    SELECT id, opened_on
    FROM warehouses
)
SELECT capacity, COUNT(*) AS n_rows, MAX(opened_on) AS latest_opened_on
FROM combined
GROUP BY capacity
ORDER BY capacity
