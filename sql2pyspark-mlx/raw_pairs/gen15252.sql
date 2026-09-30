select location
from departments
where budget is not null
union all
select location
from departments
where location = 'Singapore'
