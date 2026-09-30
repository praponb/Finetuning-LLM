SELECT full_name FROM employees WHERE hire_date BETWEEN '2024-04-20' AND '2025-04-01' INTERSECT SELECT full_name FROM employees WHERE manager_id = 12;
