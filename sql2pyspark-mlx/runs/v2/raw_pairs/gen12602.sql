SELECT *, ADD_MONTHS(launch_date, 1) launch_date_plus_months, YEAR(launch_date) launch_date_year FROM products WHERE price IS NULL AND cost BETWEEN 240 AND 280
