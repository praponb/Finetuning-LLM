SELECT amount, 'payment' AS source FROM payments WHERE HOUR(paid_at) BETWEEN 10 AND 13 UNION SELECT cost, 'shipment' AS source FROM shipments WHERE YEAR(delivered_date) >= 2024 ORDER BY amount DESC;
