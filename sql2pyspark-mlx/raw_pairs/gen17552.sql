SELECT carrier, COUNT(CASE WHEN carrier = 'FedEx' THEN 1 END) AS fedex_count FROM shipments GROUP BY carrier
