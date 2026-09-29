select * from (select case when budget >= 3383000 then 'high' when budget >= 1870000 then 'mid' else 'low' end as budget_bucket, location, budget from departments)
pivot (sum(budget) for location in ('New York', 'Tokyo', 'Bangkok', 'Berlin'))
order by budget_bucket;
