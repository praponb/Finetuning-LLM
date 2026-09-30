select
    quarter(shipped_date) as shipped_quarter,
    case when cost >= 105 then 'high' when cost >= 43 then 'mid' else 'low' end as cost_bucket,
    count(case when carrier = 'Kerry' then 1 end) as kerry_count,
    round(stddev(cost), 2) as stddev_cost
from shipments
group by quarter(shipped_date), case when cost >= 105 then 'high' when cost >= 43 then 'mid' else 'low' end
having stddev_cost > 26
order by stddev_cost desc;
