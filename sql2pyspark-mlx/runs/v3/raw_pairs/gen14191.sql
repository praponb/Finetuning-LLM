SELECT delivered_date FROM shipments WHERE status = 'RETURNED' UNION ALL SELECT sale_date FROM store_sales ORDER BY delivered_date
