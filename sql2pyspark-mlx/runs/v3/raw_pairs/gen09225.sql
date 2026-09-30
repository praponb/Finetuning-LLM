SELECT sale_date, 'store_sale' AS source
FROM store_sales
UNION ALL
SELECT opened_date AS sale_date, 'store' AS source
FROM stores
WHERE region = 'Central';
