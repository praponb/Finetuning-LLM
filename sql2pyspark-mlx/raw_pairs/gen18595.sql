select
    location,
    stddev_samp(budget) as budget_stddev,
    approx_percentile(budget, array(0.25, 0.5, 0.75)) as budget_quartiles,
    count_if(location = 'Berlin') as berlin_events,
    max(budget) as max_budget,
    count(case when location = 'Bangkok' then 1 end) as bangkok_count
from departments
group by location
order by budget_stddev desc nulls last
