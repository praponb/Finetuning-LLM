select
    id,
    store_id,
    case when units >= 33 then 'High' when units >= 18 then 'Medium' else 'Low' end as units_tier,
    least(revenue, units) as least_revenue_units
from store_sales
where store_id in (5, 6) and units > 31
order by least_revenue_units desc, id
limit 10;
