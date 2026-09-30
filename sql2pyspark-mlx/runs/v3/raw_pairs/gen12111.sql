SELECT segment, ROUND(age * 0.85, 2) AS age_scaled FROM customers WHERE is_active = FALSE
