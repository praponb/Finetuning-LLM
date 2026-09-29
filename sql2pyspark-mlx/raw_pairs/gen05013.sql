with filtered as (
    select
        id,
        full_name,
        email,
        dept_id,
        is_remote,
        level,
        concat(email, '/', level) email_level,
        upper(trim(full_name)) full_name_normalized
    from employees
    where level is not null
      and manager_id = 20
),
base as (
    select
        *,
        coalesce(level, 'unassigned') level_filled,
        coalesce(level, 'UNKNOWN') level_clean
    from filtered
    where level is not null
      and level in ('L3', 'L5')
),
with_dims as (
    select
        s.id,
        s.email,
        s.dept_id,
        s.is_remote,
        s.level,
        s.email_level,
        s.level_clean,
        d.location,
        d.dept_name,
        d.budget
    from base s
    left join departments d
        on s.dept_id = d.id
),
enriched as (
    select
        id,
        is_remote,
        location,
        dept_name,
        budget,
        avg(budget) over (order by budget, id rows between 2 preceding and 1 preceding) budget_avg_2_prior,
        sum(budget) over (order by budget, id rows between unbounded preceding and current row) running_budget,
        first_value(budget) ignore nulls over (partition by dept_id order by budget, id rows between unbounded preceding and current row) first_known_budget,
        avg(budget) over (order by budget, id rows between 3 preceding and 1 preceding) budget_avg_3_prior
    from with_dims
    where dept_name != 'Sales'
)
select
    id,
    is_remote,
    dept_name,
    budget,
    budget_avg_2_prior,
    first_known_budget,
    budget_avg_3_prior,
    case when dept_name rlike '^(Legal|Finance)$' then 'PRIORITY' else 'EXTENDED' end as dept_name_zone
from enriched
order by budget_avg_3_prior desc;
