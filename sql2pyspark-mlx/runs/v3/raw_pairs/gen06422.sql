select is_remote, count(*) as cnt
from employees
where lower(email) like '%ob.%'
group by is_remote
having min(salary) > 96000;
