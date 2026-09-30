SELECT COUNT(DISTINCT amount) AS unique_statuss, SUM(CASE WHEN channel = 'partner' THEN 1 ELSE 0 END) AS partner_count FROM orders WHERE shipping_fee BETWEEN 11 AND 17 OR store_id IN (29, 32);
