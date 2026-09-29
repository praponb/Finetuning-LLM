SELECT DATE_FORMAT(launch_date, 'yyyy-MM') launch_month, COUNT(stock_qty) stock_qty_count
FROM products
GROUP BY DATE_FORMAT(launch_date, 'yyyy-MM')
ORDER BY stock_qty_count;
