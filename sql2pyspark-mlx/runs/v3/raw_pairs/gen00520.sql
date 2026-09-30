select *
from (
    select carrier, min(cost) as min_cost, round(stddev(cost), 2) as stddev_cost
    from shipments
    where month(shipped_date) in (1, 3, 9)
    group by carrier
) as agg
where stddev_cost > 71
