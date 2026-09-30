with kept_keys as (
    select dept_name, id
    from departments
    except all
    select dept_name, id
    from departments
    where budget between 1913000 and 3284000
),
reconciled as (
    select s.*
    from departments s
    inner join kept_keys k on s.dept_name = k.dept_name and s.id = k.id
)
select
    case when budget >= 4465000 then 'PREMIUM' else 'STANDARD' end as budget_tier,
    dept_name,
    count_if(location = 'New York') as new_york_events,
    approx_percentile(budget, array(0.25, 0.5, 0.75)) as budget_quartiles,
    sum(case when dept_name = 'Legal' then budget else 0 end) as legal_budget,
    grouping(case when budget >= 4465000 then 'PREMIUM' else 'STANDARD' end) as gid
from reconciled
group by grouping sets ((case when budget >= 4465000 then 'PREMIUM' else 'STANDARD' end, dept_name), (case when budget >= 4465000 then 'PREMIUM' else 'STANDARD' end), ())
having legal_budget >= 0
order by budget_tier nulls first
