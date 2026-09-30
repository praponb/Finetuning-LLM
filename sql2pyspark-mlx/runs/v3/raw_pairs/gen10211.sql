WITH orders_by_store AS (
    SELECT store_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(channel))) AS channel_values
    FROM orders
    GROUP BY store_id
)
SELECT stores.region, orders_by_store.channel_values
FROM stores
JOIN orders_by_store ON stores.id = orders_by_store.store_id
ORDER BY orders_by_store.channel_values DESC, region;
