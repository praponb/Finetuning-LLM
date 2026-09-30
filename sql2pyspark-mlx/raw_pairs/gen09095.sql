select *
from (
    select date_format(delivered_date, 'yyyy-MM') as delivered_month, round(stddev(cost), 2) as stddev_cost, round(avg(cost), 2) as avg_cost
    from shipments
    group by date_format(delivered_date, 'yyyy-MM')
) as sub
where stddev_cost > 62
