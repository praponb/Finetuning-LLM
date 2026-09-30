SELECT
    s.id,
    s.region,
    s.sqft,
    t.total_count,
    t.min_order_date
FROM stores s
JOIN (SELECT store_id, COUNT(*) AS total_count, MIN(order_date) AS min_order_date
    FROM orders
    WHERE discount < 0.25
    GROUP BY store_id) t ON s.id = t.store_id
ORDER BY sqft
