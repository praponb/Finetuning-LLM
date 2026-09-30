SELECT
    customer_id,
    status,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values,
    SUM(discount) AS total_discount
FROM orders
GROUP BY customer_id, status;
