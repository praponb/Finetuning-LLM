SELECT * FROM (SELECT QUARTER(launch_date) AS launch_quarter, category, price FROM products)
PIVOT (AVG(price) FOR category IN ('Home', 'Grocery', 'Fashion', 'Books'));
