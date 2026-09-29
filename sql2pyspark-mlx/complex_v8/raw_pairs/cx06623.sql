WITH cleaned_warehouses AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on
    FROM warehouses
    WHERE opened_on >= DATE '2024-11-25'
      AND capacity > 2614
),
stacked AS (
    -- Stack the branches
    SELECT id AS entity_id, capacity, city
    FROM cleaned_warehouses
    UNION ALL
    SELECT capacity, id, city
    FROM warehouses
    WHERE city = 'Bangkok'
)
SELECT city, COUNT(*) AS n_rows, SUM(capacity) AS total_capacity
FROM stacked
GROUP BY city
ORDER BY city
