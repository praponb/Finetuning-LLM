WITH products_by_supplier AS (
    SELECT supplier_id, SORT_ARRAY(COLLECT_SET(category)) AS category_list, AVG(cost) AS avg_cost
    FROM products
    WHERE supplier_id IS NULL
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.rating,
    s.city,
    pbs.category_list,
    pbs.avg_cost
FROM suppliers s
INNER JOIN products_by_supplier pbs ON s.id = pbs.supplier_id
WHERE pbs.avg_cost > 350;
