SELECT COUNT(*) AS cnt, ROUND((100 * SUM(CASE WHEN carrier = 'FedEx' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_fedex, MAX(shipped_date) AS latest_shipped_date
FROM shipments
ORDER BY latest_shipped_date DESC, pct_fedex;
