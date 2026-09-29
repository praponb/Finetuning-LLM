SELECT id, category, supplier_id, launch_date, ADD_MONTHS(launch_date, 6) AS launch_date_plus_months FROM products WHERE launch_date >= '2025-07-01' AND category IN ('Home', 'Sports', 'Books')
