SELECT brand, launch_date, price FROM products WHERE brand NOT IN ('Umbrella', 'Acme', 'Initech') AND supplier_id IS NULL ORDER BY launch_date
