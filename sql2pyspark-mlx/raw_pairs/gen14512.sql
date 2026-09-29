SELECT
    suppliers.id,
    suppliers.city,
    t.min_price,
    t.category_list
FROM suppliers
INNER JOIN (SELECT supplier_id, MIN(price) AS min_price, SORT_ARRAY(COLLECT_SET(category)) AS category_list
    FROM products
    WHERE cost > 310
    GROUP BY supplier_id) t ON suppliers.id = t.supplier_id
WHERE t.min_price > 870
ORDER BY suppliers.id
LIMIT 1
