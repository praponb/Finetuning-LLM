SELECT p.id, p.launch_date, t.avg_quantity
FROM products p
LEFT OUTER JOIN (SELECT product_id, ROUND(AVG(quantity), 2) AS avg_quantity
    FROM inventory
    GROUP BY product_id) t ON p.id = t.product_id
WHERE t.avg_quantity >= 26
ORDER BY launch_date;
