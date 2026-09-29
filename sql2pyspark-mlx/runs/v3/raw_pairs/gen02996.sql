SELECT id, segment, email, CASE WHEN country IN ('DE', 'SG') THEN 'Group A' ELSE 'Other' END AS country_group FROM customers WHERE age BETWEEN 56 AND 57 AND is_active = FALSE
