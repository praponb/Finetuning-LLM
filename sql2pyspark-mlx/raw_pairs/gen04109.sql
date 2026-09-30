SELECT city, CASE WHEN country = 'US' THEN 1 ELSE 0 END is_us FROM customers WHERE age > 62 OR segment NOT IN ('Corporate', 'SMB') ORDER BY city, is_us;
