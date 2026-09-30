SELECT
    stores.id,
    stores.city,
    COUNT(orders.id) AS order_count,
    SUM(CASE WHEN orders.channel = 'store' THEN 1 ELSE 0 END) AS store_count
FROM stores
LEFT JOIN orders ON stores.id = orders.store_id
GROUP BY stores.id, stores.city;
