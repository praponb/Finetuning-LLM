select
    date_trunc('week', sale_date) as week_start,
    store_id,
    case when revenue >= 4300 then 'HIGH' else 'LOW' end as revenue_tier,
    round(avg(units), 2) as avg_units,
    corr(revenue, units) as corr_revenue_units,
    stddev_samp(revenue) as revenue_stddev
from store_sales
group by date_trunc('week', sale_date), store_id, case when revenue >= 4300 then 'HIGH' else 'LOW' end
order by week_start nulls first;
