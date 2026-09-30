select case when budget >= 4265000 then 'high' when budget >= 1239000 then 'mid' else 'low' end as budget_bucket, count(case when location = 'Tokyo' then 1 end) as tokyo_count
from departments
where budget <= 1793000
group by case when budget >= 4265000 then 'high' when budget >= 1239000 then 'mid' else 'low' end
having count(case when location = 'Tokyo' then 1 end) >= 2
order by tokyo_count, budget_bucket
limit 5
