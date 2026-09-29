SELECT channel, SUM(CASE WHEN status = 'RETURNED' THEN shipping_fee ELSE 0 END) AS returned_shipping_fee, MIN(amount) AS min_amount FROM orders GROUP BY channel ORDER BY returned_shipping_fee DESC;
