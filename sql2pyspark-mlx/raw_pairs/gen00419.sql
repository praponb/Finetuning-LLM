SELECT revenue
FROM store_sales
WHERE revenue <= 2300
UNION ALL
SELECT shipping_fee AS revenue
FROM orders;
