SELECT status, SORT_ARRAY(COLLECT_SET(status)) AS status_list, SUM(CASE WHEN carrier = 'FedEx' THEN 1 ELSE 0 END) AS fedex_count FROM shipments WHERE MONTH(shipped_date) IN (3, 4, 12) GROUP BY status
