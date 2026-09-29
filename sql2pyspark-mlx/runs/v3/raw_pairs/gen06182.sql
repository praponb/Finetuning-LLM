select dept_name, round(stddev(budget), 2) as stddev_budget, count(budget) as budget_count
from departments
group by dept_name
having round(stddev(budget), 2) >= 73
