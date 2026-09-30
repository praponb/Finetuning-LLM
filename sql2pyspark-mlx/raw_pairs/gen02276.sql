with enriched as (
    select
        *,
        avg(salary) over (order by salary, id rows between 6 preceding and current row) as salary_avg_6,
        avg(salary) over (partition by job_title, dept_id order by salary, id rows between 3 preceding and 1 preceding) as salary_avg_3_prior,
        row_number() over w_grp as event_seq
    from employees
    where email like '%@startup.io'
    window w_grp as (partition by job_title order by hire_date, id)
),
latest as (
    select *, row_number() over (partition by job_title order by event_seq desc, id) as row_num
    from enriched
    qualify row_num = 1
),
kept_keys as (
    select manager_id, job_title
    from latest
    intersect
    select manager_id, job_title
    from employees
    where length(email) < 14
),
kept_rows as (
    select s.*
    from latest s
    join kept_keys k on s.manager_id = k.manager_id and s.job_title = k.job_title
),
enriched_employees as (
    select src.*, b.budget
    from kept_rows src
    left join departments b
        on src.dept_id = b.id
),
joined as (
    select a.*, b.level as employee_level
    from enriched_employees a
    left join employees b
        on a.manager_id <=> b.id
),
stats as (
    select
        date_trunc('year', hire_date) as year_start,
        level,
        approx_percentile(salary_avg_6, array(0.25, 0.5, 0.75)) as salary_avg_6_quartiles,
        count_if(level = 'L2') as l2_events,
        rank() over (partition by date_trunc('year', hire_date) order by count_if(level = 'L2') desc) as l2_events_rank
    from joined
    group by 1, 2
)
select
    year_start,
    level,
    salary_avg_6_quartiles,
    l2_events_rank
from stats
order by year_start desc, level;
