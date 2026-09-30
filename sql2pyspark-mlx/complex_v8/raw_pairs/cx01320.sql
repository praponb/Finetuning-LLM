WITH scoped AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        opened_on
    FROM warehouses
    WHERE LOWER(wh_code) LIKE '%bk%'
)
SELECT id, opened_on, 'warehouse' AS origin
FROM scoped
WHERE opened_on >= DATE '2024-11-25'
  AND city = 'Singapore'
UNION ALL
SELECT id, opened_on, 'warehouse_2' AS origin
FROM warehouses
UNION ALL
SELECT capacity AS id, opened_on, 'warehouse_3' AS origin
FROM warehouses
WHERE LENGTH(TRIM(wh_code)) >= 7
