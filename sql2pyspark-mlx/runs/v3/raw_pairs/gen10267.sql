SELECT amount
FROM orders
UNION ALL
SELECT revenue
FROM store_sales;
