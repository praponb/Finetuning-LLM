select round((100 * sum(case when dept_name = 'Legal' then 1 else 0 end)) / count(*), 1) pct_legal, max(budget) - min(budget) budget_range
from departments;
