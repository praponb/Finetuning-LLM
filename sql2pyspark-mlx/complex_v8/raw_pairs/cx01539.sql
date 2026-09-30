WITH base AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        DATE_FORMAT(opened_on, 'yyyy-MM') AS opened_on_label
    FROM warehouses
    WHERE opened_on < DATE '2025-07-03'
      AND capacity >= 2614
)
SELECT id, city, 'warehouse' AS src
FROM base
WHERE LENGTH(TRIM(wh_code)) >= 4
UNION ALL
SELECT sku_id AS id, operator AS city, 'move' AS src
FROM stock_moves
UNION ALL
SELECT id, category AS city, 'sku' AS src
FROM skus
WHERE LOWER(sku_code) LIKE '%ap%'
ORDER BY city
