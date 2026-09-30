with valid_shipments as (
    select *
    from shipments
    where status != 'DELIVERED' and (cost between 26 and 47 or cost between 30 and 76)
),
cost_bucket_summary as (
    select case when cost >= 81 then 'high' when cost >= 38 then 'mid' else 'low' end as cost_bucket, min(cost) as min_cost
    from valid_shipments
    group by case when cost >= 81 then 'high' when cost >= 38 then 'mid' else 'low' end
)
select cost_bucket, min_cost
from cost_bucket_summary
order by min_cost, cost_bucket
limit 20;
