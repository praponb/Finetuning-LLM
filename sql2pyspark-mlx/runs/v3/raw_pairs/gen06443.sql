select *, case when salary >= 147000 then 'High' when salary >= 41000 then 'Medium' else 'Low' end as salary_band, lower(full_name) as full_name_lower
from employees
where email like '%@company.co' and dept_id = 40
