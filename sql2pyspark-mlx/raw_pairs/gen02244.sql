select
    carrier,
    cost_bucket,
    total_cost,
    pct_delivered
from (
    select
        carrier,
        case when cost >= 98 then 'high' when cost >= 15 then 'mid' else 'low' end as cost_bucket,
        sum(cost) as total_cost,
        round((100 * sum(case when status = 'DELIVERED' then 1 else 0 end)) / count(*), 1) as pct_delivered
    from shipments
    group by carrier, case when cost >= 98 then 'high' when cost >= 15 then 'mid' else 'low' end
) as sub
where total_cost >= 450
order by pct_delivered desc, carrier, cost_bucket
limit 5;
