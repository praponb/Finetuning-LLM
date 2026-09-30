select * from (select case when budget >= 3089000 then 'high' when budget >= 1095000 then 'mid' else 'low' end as budget_bucket, dept_name, budget from departments)
pivot (count(budget) for dept_name in ('Finance', 'Legal', 'HR'));
