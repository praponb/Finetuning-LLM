WITH shipments_by_order AS (
    SELECT order_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(carrier))) AS carrier_values
    FROM shipments
    WHERE carrier != 'FedEx'
    GROUP BY order_id
)
SELECT o.store_id, o.discount, sbo.carrier_values
FROM orders o
LEFT JOIN shipments_by_order sbo ON o.id = sbo.order_id;
