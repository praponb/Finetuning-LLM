select location, dept_name from departments where budget <= 1105000 union all select location, dept_name from departments where dept_name in ('Sales', 'Engineering', 'Marketing') order by dept_name
