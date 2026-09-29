SELECT
    s.id,
    s.city,
    t.total_amount,
    t.pct_shipped
FROM stores s
JOIN (SELECT store_id, SUM(amount) AS total_amount, ROUND((100 * SUM(CASE WHEN status = 'SHIPPED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_shipped
    FROM orders
    WHERE order_date > '2024-11-01'
    GROUP BY store_id) t ON s.id = t.store_id
WHERE t.pct_shipped > 59
ORDER BY id;
