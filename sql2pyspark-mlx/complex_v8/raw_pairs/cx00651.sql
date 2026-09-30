WITH cleaned_warehouses AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        capacity,
        opened_on,
        DATEDIFF(DATE '2025-12-31', opened_on) AS days_since_opened_on
    FROM warehouses
    WHERE (city = 'Singapore' OR LOWER(wh_code) LIKE '%ul%')
),
stacked AS (
    -- Stack the branches
    SELECT id AS qty, wh_code AS name
    FROM cleaned_warehouses
    WHERE capacity BETWEEN 2614 AND 11506
      AND opened_on >= DATE '2024-04-13'
    UNION ALL
    SELECT id, category
    FROM skus
)
SELECT name, COUNT(*) AS n_rows, SUM(qty) AS total_qty
FROM stacked
GROUP BY name
ORDER BY name
