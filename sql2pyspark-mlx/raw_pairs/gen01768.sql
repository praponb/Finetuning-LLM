SELECT id, launch_date, brand, price, MAX(price) OVER (PARTITION BY YEAR(launch_date), brand) AS launch_date_year_max_price FROM products WHERE ARRAY_CONTAINS(tags, 'clearance') ORDER BY price ASC;
