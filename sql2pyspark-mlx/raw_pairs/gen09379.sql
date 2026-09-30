select
    date_format(hire_date, 'yyyy-MM') hire_month,
    sum(salary) total_salary,
    count(*) cnt,
    sum(sum(salary)) over (order by date_format(hire_date, 'yyyy-MM')) as cumulative_salary
from employees
where job_title = 'Manager'
group by date_format(hire_date, 'yyyy-MM')
order by cnt desc
