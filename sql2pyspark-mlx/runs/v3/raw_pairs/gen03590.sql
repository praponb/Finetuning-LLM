SELECT status, SUM(CASE WHEN carrier = 'FedEx' THEN 1 ELSE 0 END) fedex_count FROM shipments GROUP BY status ORDER BY fedex_count DESC, status LIMIT 20
