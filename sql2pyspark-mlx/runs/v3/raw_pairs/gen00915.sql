SELECT segment, city, country, SUM(CASE WHEN segment = 'Corporate' THEN 1 ELSE 0 END) corporate_count FROM customers GROUP BY segment, city, country ORDER BY country
