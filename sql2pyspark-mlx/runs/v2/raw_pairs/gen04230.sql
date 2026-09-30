select location, budget from departments where not (budget between 2511000 and 3358000 and dept_name in ('HR', 'Sales', 'Marketing'))
