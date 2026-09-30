SELECT s.sqft, s.region, t.min_order_date
FROM stores s
LEFT OUTER JOIN (SELECT store_id, MIN(order_date) AS min_order_date
    FROM orders
    WHERE store_id IN (6, 31)
    GROUP BY store_id) t ON s.id = t.store_id
ORDER BY sqft DESC;
