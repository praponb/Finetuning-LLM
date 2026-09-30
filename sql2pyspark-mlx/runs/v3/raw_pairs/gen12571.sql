select
    dept_name,
    sum(case when location = 'Bangkok' then budget else 0 end) as bangkok_budget,
    count(distinct budget) as unique_locations,
    round((100 * sum(case when location = 'Bangkok' then 1 else 0 end)) / count(*), 1) as pct_bangkok
from departments
group by dept_name
having count(distinct budget) >= 2;
