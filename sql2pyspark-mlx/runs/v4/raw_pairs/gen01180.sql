SELECT MONTH(launch_date) AS launch_month, ROUND(STDDEV(price), 2) AS stddev_price, ROUND(AVG(stock_qty), 2) AS avg_stock_qty
FROM products
WHERE cost BETWEEN 200 AND 630 AND product_name LIKE '%Hub%'
GROUP BY 1;
