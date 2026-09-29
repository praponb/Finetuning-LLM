select quarter(start_date) as start_quarter, round((100 * sum(case when plan = 'enterprise' then 1 else 0 end)) / count(*), 1) as pct_enterprise, concat_ws(', ', sort_array(collect_set(plan))) as plan_values
from subscriptions
group by 1
order by 3 desc, pct_enterprise desc;
