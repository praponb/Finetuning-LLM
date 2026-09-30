SELECT hire_date FROM employees WHERE hire_date IS NOT NULL INTERSECT SELECT opened_date FROM stores WHERE opened_date IS NOT NULL
