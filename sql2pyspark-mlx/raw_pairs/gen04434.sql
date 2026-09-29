WITH OrderStats AS (
    SELECT order_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
    FROM payments
    GROUP BY order_id
)
SELECT o.customer_id, o.order_date, O.status_values
FROM orders o
INNER JOIN OrderStats O ON o.id = O.order_id;
