SELECT status, channel, COUNT(CASE WHEN status = 'RETURNED' THEN 1 END) AS returned_count, AVG(shipping_fee) AS avg_shipping_fee FROM orders GROUP BY status, channel ORDER BY avg_shipping_fee DESC
