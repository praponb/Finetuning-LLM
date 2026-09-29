select
    dept_name,
    round(stddev(budget), 2) as stddev_budget,
    count(distinct budget) as unique_dept_names,
    round((100 * sum(case when location = 'London' then 1 else 0 end)) / count(*), 1) as pct_london
from departments
group by dept_name;
