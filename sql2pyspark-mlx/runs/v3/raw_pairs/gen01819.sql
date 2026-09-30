select
    case when budget >= 4403000 then 'high' when budget >= 1344000 then 'mid' else 'low' end as budget_bucket,
    dept_name,
    location,
    count(case when location = 'Chiang Mai' then 1 end) as chiang_mai_count
from departments
group by case when budget >= 4403000 then 'high' when budget >= 1344000 then 'mid' else 'low' end, dept_name, location;
