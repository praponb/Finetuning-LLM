SELECT 'customer' AS record_type, signup_date
FROM customers
UNION
SELECT 'shipment' AS record_type, delivered_date AS signup_date
FROM shipments
ORDER BY signup_date;
