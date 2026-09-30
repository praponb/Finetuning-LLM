select dept_name, max(budget) - min(budget) as budget_range, count(*) as total_count from departments group by dept_name order by total_count desc, dept_name asc
