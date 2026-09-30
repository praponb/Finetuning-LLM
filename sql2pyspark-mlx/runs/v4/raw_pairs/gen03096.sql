with cleaned as (
    select
        *,
        trim(level) as clean_level,
        upper(job_title) as job_title_upper
    from employees
    where hire_date >= date '2023-03-14'
      and job_title is not null
),
scored as (
    select
        *,
        rank() over (partition by level order by salary) as salary_rank,
        avg(salary) over (order by salary, id rows between 3 preceding and current row) as salary_avg_3,
        sum(salary) over (partition by dept_id order by hire_date, id rows between unbounded preceding and current row) as cumulative_salary,
        lag(salary, 1) over (partition by manager_id order by salary, id) as prev_salary,
        avg(salary) over (partition by dept_id) as dept_id_avg_salary
    from cleaned
),
latest as (
    select *
    from scored
    qualify row_number() over (partition by dept_id, job_title order by salary_avg_3 desc, id) = 1
)
select
    case when salary_rank >= 7 then 'LARGE' else 'SMALL' end as salary_rank_tier,
    date_trunc('year', hire_date) as year_start,
    count(distinct level) as distinct_levels,
    sum(case when level = 'L3' then salary_avg_3 else 0 end) as l3_salary_avg_3
from latest
group by rollup (case when salary_rank >= 7 then 'LARGE' else 'SMALL' end, date_trunc('year', hire_date))
order by year_start nulls last;
