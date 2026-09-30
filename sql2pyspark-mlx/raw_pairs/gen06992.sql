SELECT method, 'payment' AS kind FROM payments WHERE status = 'FAILED' UNION ALL SELECT store_name, 'store' AS kind FROM stores;
