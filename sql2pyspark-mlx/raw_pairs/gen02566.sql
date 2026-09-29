SELECT location, 'department' AS record_type FROM departments UNION ALL SELECT city, 'store' AS record_type FROM stores WHERE opened_date BETWEEN '2023-04-01' AND '2025-07-01'
