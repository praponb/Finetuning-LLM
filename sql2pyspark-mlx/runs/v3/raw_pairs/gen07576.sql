SELECT store_id FROM orders WHERE discount IS NOT NULL UNION ALL SELECT store_id FROM orders WHERE channel IN ('mobile', 'partner')
