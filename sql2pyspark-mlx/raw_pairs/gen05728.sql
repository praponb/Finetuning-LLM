select
    case when budget >= 3045000 then 'high' when budget >= 802000 then 'mid' else 'low' end budget_bucket,
    location,
    sum(budget) total_budget,
    count(case when location = 'Chiang Mai' then 1 end) chiang_mai_count
from departments
where budget >= 1209000
group by budget_bucket, location
having count(case when location = 'Chiang Mai' then 1 end) < 4
order by chiang_mai_count desc;
