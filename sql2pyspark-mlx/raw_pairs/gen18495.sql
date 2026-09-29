SELECT QUARTER(launch_date) launch_quarter, MAX(price) max_price FROM products GROUP BY launch_quarter HAVING MAX(price) > 1010;
