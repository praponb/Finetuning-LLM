SELECT id, country, credit_limit, city, LOWER(segment) AS segment_lower FROM customers WHERE (credit_limit > 14300 AND city IN ('London', 'New York', 'Bangkok')) OR is_active = TRUE;
