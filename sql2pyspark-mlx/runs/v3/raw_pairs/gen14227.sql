SELECT signup_date, LOWER(segment) AS segment_lower FROM customers WHERE credit_limit > 9600 AND is_active = TRUE AND country = 'UK' ORDER BY signup_date;
