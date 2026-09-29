SELECT carrier, order_id, MIN(delivered_date) AS first_delivered_date FROM shipments WHERE status = 'LOST' GROUP BY carrier, order_id ORDER BY first_delivered_date, order_id DESC;
