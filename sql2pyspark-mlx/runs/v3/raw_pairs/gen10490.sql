SELECT SUM(CASE WHEN channel = 'partner' THEN 1 ELSE 0 END) AS partner_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
FROM orders
WHERE channel IN ('store', 'web') AND status IN ('CANCELLED', 'RETURNED', 'SHIPPED') AND amount IS NOT NULL
ORDER BY status_values
