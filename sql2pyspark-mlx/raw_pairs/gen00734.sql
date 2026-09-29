select is_remote, sum(salary) as total_salary, round(sum(salary) * 100.0 / sum(sum(salary)) over (), 2) as pct_of_total
from employees
where lower(email) like '%lic%'
group by is_remote;
