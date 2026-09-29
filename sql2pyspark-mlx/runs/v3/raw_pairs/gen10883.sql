SELECT CASE WHEN status IN ('SUCCESS', 'REFUNDED') THEN 'priority' ELSE 'other' END AS status_group, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values, MIN(amount) AS min_amount
FROM payments
WHERE paid_at >= '2023-07-01' OR order_id IN (12, 14, 15, 36)
GROUP BY CASE WHEN status IN ('SUCCESS', 'REFUNDED') THEN 'priority' ELSE 'other' END;
