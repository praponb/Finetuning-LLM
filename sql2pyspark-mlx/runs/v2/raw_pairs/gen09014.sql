SELECT
    s.id,
    s.country,
    s.supplier_name,
    t.category_values,
    t.books_count
FROM suppliers s
LEFT OUTER JOIN (SELECT supplier_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(category))) AS category_values, SUM(CASE WHEN category = 'Books' THEN 1 ELSE 0 END) AS books_count
    FROM products
    WHERE stock_qty >= 144
    GROUP BY supplier_id) t ON s.id = t.supplier_id
ORDER BY 2 DESC
