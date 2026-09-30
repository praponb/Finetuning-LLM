SELECT d.dept_name, d.budget, d.location, e.email, ROUND(d.budget, 0) AS budget_rounded FROM departments d JOIN employees e ON d.id = e.dept_id WHERE e.salary > 119000;
