SELECT order_id, avg_amount, method_values
FROM (
    SELECT order_id, ROUND(AVG(amount), 2) AS avg_amount, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(method))) AS method_values
    FROM payments
    GROUP BY order_id
) AS t
WHERE avg_amount >= 64
ORDER BY order_id DESC;
