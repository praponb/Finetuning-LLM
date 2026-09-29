SELECT
    s.id,
    s.region,
    s.store_name,
    t.avg_amount,
    t.stddev_amount
FROM stores s
LEFT JOIN (SELECT store_id, ROUND(AVG(amount), 2) AS avg_amount, ROUND(STDDEV(amount), 2) AS stddev_amount
    FROM orders
    WHERE customer_id = 17
    GROUP BY store_id) t ON s.id = t.store_id
WHERE t.stddev_amount > 78
