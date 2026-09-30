SELECT 'store_sale' AS kind, revenue FROM store_sales WHERE revenue < 1200 UNION ALL SELECT 'payment' AS kind, amount FROM payments;
