SELECT s.city, s.sqft, t.latest_order_date
FROM stores s
LEFT JOIN (SELECT store_id, MAX(order_date) AS latest_order_date
    FROM orders
    GROUP BY store_id) t ON s.id = t.store_id;
