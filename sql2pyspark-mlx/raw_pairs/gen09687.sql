SELECT carrier, MONTH(delivered_date) delivered_month, SUM(CASE WHEN carrier = 'FedEx' THEN 1 ELSE 0 END) fedex_count FROM shipments GROUP BY carrier, delivered_month
