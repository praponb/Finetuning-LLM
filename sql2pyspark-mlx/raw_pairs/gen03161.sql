select
    date_trunc('week', launch_date) as week_start,
    approx_percentile(cost, 0.9) as cost_p90,
    count_if(brand = 'Initech') as initech_events
from products
group by week_start
having count_if(brand = 'Initech') < 1000
order by initech_events desc
