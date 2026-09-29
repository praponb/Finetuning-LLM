SELECT tags, launch_date, price
FROM products
WHERE brand IN ('Umbrella', 'Acme', 'Initech') AND (launch_date < '2024-03-01' OR brand = 'Acme');
