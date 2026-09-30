select count(case when dept_name = 'Legal' then 1 end) as legal_count
from departments
where location is not null;
