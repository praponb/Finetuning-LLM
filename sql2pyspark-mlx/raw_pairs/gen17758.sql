SELECT budget, dept_name, location FROM departments WHERE (location <> 'New York' AND budget > 3857000) OR dept_name = 'HR' ORDER BY location;
