SELECT id, CASE WHEN age >= 30 THEN 'Senior' ELSE 'Junior' END AS age_group FROM customers;
