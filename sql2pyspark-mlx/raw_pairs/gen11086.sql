select
    dept_id,
    approx_percentile(salary, array(0.25, 0.5, 0.75)) as salary_quartiles,
    max(salary) filter (where job_title = 'Director') as director_max_salary,
    grouping(dept_id) as grouping_id
from employees
group by rollup (dept_id)
order by dept_id nulls last;
