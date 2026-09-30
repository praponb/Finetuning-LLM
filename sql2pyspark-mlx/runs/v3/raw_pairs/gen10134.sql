SELECT *, CASE WHEN dept_name = 'HR' THEN 1 ELSE 0 END AS is_hr FROM departments WHERE budget >= 3367000
