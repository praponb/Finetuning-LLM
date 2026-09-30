select location, case when budget >= 4340000 then 'high' when budget >= 878000 then 'mid' else 'low' end as budget_bucket, count(budget) as budget_count
from departments
where location != 'Singapore' and budget between 1396000 and 2904000
group by 1, 2
