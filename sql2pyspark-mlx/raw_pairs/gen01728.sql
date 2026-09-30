SELECT 'store_sale' AS record_type, sale_date FROM store_sales WHERE sale_date IS NOT NULL UNION ALL SELECT 'shipment' AS record_type, shipped_date FROM shipments WHERE shipped_date IS NOT NULL
