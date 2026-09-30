SELECT is_active, country, SUM(CASE WHEN city = 'Berlin' THEN credit_limit ELSE 0 END) AS berlin_credit_limit FROM customers GROUP BY CUBE(is_active, country);
