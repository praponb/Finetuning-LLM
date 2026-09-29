select shipped_date
from shipments
where shipped_date is not null
union all
select hire_date
from employees
where hire_date is not null
order by shipped_date;
