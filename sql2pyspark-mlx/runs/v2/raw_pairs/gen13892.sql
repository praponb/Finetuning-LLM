select *
from (
    select
        year(hire_date) as hire_year,
        date_trunc('MONTH', hire_date) as hire_month,
        count(*) as total_count,
        sort_array(collect_set(job_title)) as job_title_list
    from employees
    where hire_date <= '2025-06-01'
    group by year(hire_date), date_trunc('MONTH', hire_date)
) as agg
where total_count > 7
order by total_count desc, hire_year, hire_month
limit 5
