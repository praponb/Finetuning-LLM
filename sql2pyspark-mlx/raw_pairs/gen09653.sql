SELECT carrier, SORT_ARRAY(COLLECT_SET(status)) AS status_list
FROM shipments
WHERE order_id IS NOT NULL AND status IN ('LOST', 'RETURNED')
GROUP BY carrier
ORDER BY carrier;
