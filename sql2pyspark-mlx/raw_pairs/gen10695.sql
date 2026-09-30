SELECT signup_date, 'customer' AS source
FROM customers
WHERE signup_date IS NOT NULL
UNION
SELECT sale_date AS signup_date, 'store_sale' AS source
FROM store_sales
WHERE sale_date IS NOT NULL
