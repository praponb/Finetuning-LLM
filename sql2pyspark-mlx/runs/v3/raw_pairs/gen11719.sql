SELECT
    s.id,
    s.city,
    s.rating,
    t.total_price,
    t.latest_launch_date
FROM suppliers s
JOIN (SELECT supplier_id, SUM(price) AS total_price, MAX(launch_date) AS latest_launch_date
    FROM products
    GROUP BY supplier_id) t ON s.id = t.supplier_id
WHERE t.total_price > 6200;
