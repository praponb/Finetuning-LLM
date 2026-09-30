SELECT channel, store_id, SUM(CASE WHEN channel = 'partner' THEN amount ELSE 0 END) AS partner_amount FROM orders GROUP BY channel, store_id ORDER BY partner_amount DESC, store_id DESC;
