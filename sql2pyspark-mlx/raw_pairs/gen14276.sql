SELECT *, LAST_DAY(launch_date) AS launch_date_month_end FROM products WHERE launch_date >= '2023-06-08' ORDER BY price ASC, id ASC LIMIT 1;
