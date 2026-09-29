SELECT end_date, 'subscription' AS source
FROM subscriptions
WHERE monthly_fee < 120
UNION
SELECT shipped_date, 'shipment' AS source
FROM shipments
WHERE delivered_date > '2024-05-21'
ORDER BY end_date;
