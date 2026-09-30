select shipped_quarter, avg_cost, max_cost
from (
    select quarter(shipped_date) shipped_quarter, round(avg(cost), 2) avg_cost, max(cost) max_cost
    from shipments
    where order_id in (2, 11, 16, 23)
    group by quarter(shipped_date)
) s
where avg_cost < 48
order by max_cost desc, shipped_quarter
limit 10;
