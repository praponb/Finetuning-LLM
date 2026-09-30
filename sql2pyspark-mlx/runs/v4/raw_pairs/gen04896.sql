select *
from (
    select case when budget >= 3469000 then 'high' when budget >= 1484000 then 'mid' else 'low' end as budget_bucket, sum(budget) as total_budget
    from departments
    group by case when budget >= 3469000 then 'high' when budget >= 1484000 then 'mid' else 'low' end
) as sub
where total_budget > 9749000
order by budget_bucket;
