SELECT
    p.product_name,
    p.supplier_id,
    t.cnt,
    t.stddev_unit_price
FROM products p
JOIN (SELECT product_id, COUNT(*) AS cnt, ROUND(STDDEV(unit_price), 2) AS stddev_unit_price
    FROM order_items
    GROUP BY product_id) t ON p.id = t.product_id
WHERE t.stddev_unit_price >= 33
ORDER BY t.cnt
