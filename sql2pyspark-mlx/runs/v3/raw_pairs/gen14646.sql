SELECT id, name, signup_date, credit_limit, IF(is_active, 'Yes', 'No') AS is_active_label, CONCAT(segment, ' - ', country) AS segment_country FROM customers WHERE name IS NOT NULL
