select 'employee' record_type, job_title
from employees
where job_title is not null
union
select 'session' record_type, device
from sessions
where device is not null
