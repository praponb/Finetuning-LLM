SELECT order_id, amount_count, method_values
FROM (
    SELECT order_id, COUNT(amount) AS amount_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(method))) AS method_values
    FROM payments
    GROUP BY order_id
) AS s
WHERE amount_count < 9;
