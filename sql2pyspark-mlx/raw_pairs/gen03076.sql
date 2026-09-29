SELECT brand, umbrella_count FROM (SELECT brand, COUNT(CASE WHEN brand = 'Umbrella' THEN 1 END) AS umbrella_count FROM products GROUP BY brand) AS s WHERE umbrella_count >= 5;
