SELECT channel, SORT_ARRAY(COLLECT_SET(status)) status_list, MAX(amount) max_amount
FROM orders
WHERE order_date BETWEEN '2024-09-01' AND '2025-07-01' AND status IN ('CANCELLED', 'SHIPPED')
GROUP BY channel
HAVING MAX(amount) >= 1280
ORDER BY max_amount ASC
