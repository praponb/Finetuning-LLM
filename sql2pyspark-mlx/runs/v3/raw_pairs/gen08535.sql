SELECT signup_date AS event_date, 'customer' AS source FROM customers WHERE LOWER(email) LIKE '%rac%' UNION SELECT delivered_date AS event_date, 'shipment' AS source FROM shipments;
