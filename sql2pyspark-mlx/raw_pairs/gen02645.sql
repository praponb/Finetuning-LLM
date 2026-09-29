SELECT sale_date
FROM store_sales
UNION
SELECT signup_date
FROM customers;
