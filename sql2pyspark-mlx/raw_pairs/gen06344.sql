SELECT email, INITCAP(TRIM(name)) AS name_title, LOWER(segment) AS segment_lower FROM customers WHERE city IN ('Bangkok', 'Singapore')
