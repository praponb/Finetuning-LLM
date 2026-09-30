with valid as (
    -- Keep recent, valid rows
    select *, case when job_title in ('Designer', 'Manager') then 'Group A' else 'Other' end as job_title_group
    from employees
    where hire_date >= date '2023-05-05'
      and job_title is not null
),
joined as (
    -- Attach departments attributes
    select s.*, b.budget
    from valid s
    left join departments b
        on s.dept_id = b.id
    where b.dept_name in ('Marketing', 'HR', 'Engineering')
),
deduped as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by level order by salary desc, id asc) as dedup_rank
    from joined
    qualify dedup_rank = 1
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        full_name,
        email,
        manager_id,
        level,
        job_title_group,
        budget,
        dedup_rank,
        coalesce(full_name, 'N/A') as full_name_filled,
        coalesce(job_title, 'DIRECT') as job_title_filled
    from deduped
    where level is not null
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        dedup_rank + budget as dedup_rank_plus_budget,
        budget * dedup_rank as budget_times_dedup_rank,
        coalesce(level, 'DIRECT') as level_filled
    from filtered
    where manager_id is not null
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        max(dedup_rank_plus_budget) over (partition by manager_id order by budget_times_dedup_rank asc, id asc rows between 2 preceding and current row) as last3_max_dedup_rank_plus_budget,
        sum(dedup_rank_plus_budget) over (partition by manager_id order by budget_times_dedup_rank asc, id asc rows between 2 preceding and current row) as last3_sum_dedup_rank_plus_budget,
        avg(dedup_rank_plus_budget) over (partition by manager_id order by budget_times_dedup_rank asc, id asc rows between 2 preceding and current row) as last3_avg_dedup_rank_plus_budget
    from cleaned
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        id,
        level,
        budget,
        last3_sum_dedup_rank_plus_budget
    from scored
) src
unpivot (
    val for metric_name in (budget, last3_sum_dedup_rank_plus_budget)
) long_rows;
