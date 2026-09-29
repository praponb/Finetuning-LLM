WITH products_by_supplier AS (
    SELECT supplier_id, COUNT(*) AS cnt, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(brand))) AS brand_values
    FROM products
    GROUP BY supplier_id
)
SELECT s.supplier_name, pbs.cnt, pbs.brand_values
FROM suppliers s
INNER JOIN products_by_supplier pbs ON s.id = pbs.supplier_id
WHERE pbs.cnt > 5
