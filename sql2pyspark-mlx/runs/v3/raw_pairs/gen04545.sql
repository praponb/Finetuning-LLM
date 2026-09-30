select case when budget >= 3346000 then 'high' when budget >= 1816000 then 'mid' else 'low' end as budget_bucket, round(stddev(budget), 2) as stddev_budget
from departments
where budget > 1973000 and budget <= 2380000
group by case when budget >= 3346000 then 'high' when budget >= 1816000 then 'mid' else 'low' end;
