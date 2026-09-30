WITH orders_by_store AS (
    SELECT store_id, COUNT(discount) AS discount_count
    FROM orders
    WHERE channel = 'web'
    GROUP BY store_id
)
SELECT stores.id, stores.region, orders_by_store.discount_count
FROM stores
LEFT JOIN orders_by_store ON stores.id = orders_by_store.store_id
WHERE orders_by_store.discount_count > 5
ORDER BY region, stores.id;
