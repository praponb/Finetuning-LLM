with kept_keys as (
    -- Set intersection on keys
    select job_title, level
    from employees
    intersect all
    select job_title, level
    from employees
    where salary < 74000
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from employees s
    inner join kept_keys k on s.job_title = k.job_title and s.level = k.level
)
-- Multi-level aggregation
select
    case when salary >= 138000 then 'LARGE' else 'SMALL' end as salary_tier,
    max(salary) as max_salary,
    round(sum(salary) / nullif(count(case when job_title = 'Intern' then 1 end), 0), 2) as salary_per_intern,
    size(collect_set(level)) as num_levels,
    sort_array(collect_set(level)) as level_list,
    sum(case when job_title = 'Manager' then salary else 0 end) as manager_salary,
    sort_array(collect_set(level)) as level_list_2
from reconciled
group by case when salary >= 138000 then 'LARGE' else 'SMALL' end
order by salary_per_intern asc nulls last;
