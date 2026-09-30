select *
from (
    select
        date_trunc('MONTH', hire_date) as hire_month,
        is_remote,
        count(distinct salary) as unique_managers,
        sum(salary) as total_salary
    from employees
    group by date_trunc('MONTH', hire_date), is_remote
) as x
where total_salary > 590000
order by hire_month desc, is_remote asc
limit 10;
