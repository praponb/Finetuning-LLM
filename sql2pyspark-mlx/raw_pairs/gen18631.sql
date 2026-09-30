SELECT id, name, age, LOWER(city) AS city_lower FROM customers WHERE (country != 'SG' AND signup_date >= '2025-07-01') OR is_active;
