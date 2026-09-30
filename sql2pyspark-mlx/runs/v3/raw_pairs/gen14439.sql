SELECT stock_qty, ADD_MONTHS(launch_date, 3) AS launch_date_plus_months, ARRAY_JOIN(tags, ', ') AS tags_text FROM products
