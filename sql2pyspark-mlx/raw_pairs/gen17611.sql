SELECT status, TRIM(status) AS clean_status
FROM shipments
WHERE NOT (order_id IN (3, 17, 19, 32) AND delivered_date <= '2023-10-01' AND shipped_date BETWEEN '2023-05-01' AND '2025-01-17')
ORDER BY clean_status ASC
