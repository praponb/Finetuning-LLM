select dept_name, count(case when dept_name = 'Finance' then 1 end) as finance_count, count(budget) as budget_count from departments group by dept_name order by finance_count desc, dept_name
