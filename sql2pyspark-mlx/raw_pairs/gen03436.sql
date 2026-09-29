select *, case when salary >= 143000 then 'High' when salary >= 70000 then 'Medium' else 'Low' end as salary_band, round(salary * 100, 2) as salary_adjusted
from employees
