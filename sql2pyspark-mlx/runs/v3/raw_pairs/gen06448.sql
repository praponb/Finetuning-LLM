select
    order_id,
    carrier,
    round((100 * sum(case when carrier = 'Flash' then 1 else 0 end)) / count(*), 1) as pct_flash,
    avg(cost) as avg_cost,
    sum(case when carrier = 'FedEx' then cost else 0 end) as fedex_cost
from shipments
where carrier is null
group by order_id, carrier;
