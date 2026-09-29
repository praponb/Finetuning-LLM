with eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from orders s
    where not exists (
            select 1
            from shipments ch
            where ch.order_id = s.id
              and ch.shipped_date < s.order_date
        )
),
base as (
    -- Basic cleansing and defaults
    select *, case when status = 'COMPLETED' then 1 else 0 end as is_completed
    from eligible
    where customer_id is not null
      and shipping_fee < 21
),
eligible_orders as (
    -- Anti-join via a correlated subquery
    select s.*
    from base s
    where not exists (
            select 1
            from shipments c
            where c.order_id = s.id
              and c.carrier in ('DHL', 'Flash', 'Kerry')
        )
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        stddev_samp(shipping_fee) over (partition by channel) as channel_stddev_samp_shipping_fee,
        lag(is_completed, 1) over (partition by customer_id, status order by discount, id) as prev_is_completed
    from eligible_orders
)
-- Aggregate with conditional metrics
select
    case when prev_is_completed >= 83 then 'LARGE' else 'SMALL' end as prev_is_completed_tier,
    case when shipping_fee >= 38 then 'PREMIUM' else 'STANDARD' end as shipping_fee_tier,
    count(distinct store_id) as distinct_store_ids,
    round(avg(channel_stddev_samp_shipping_fee), 2) as avg_channel_stddev_samp_shipping_fee,
    sum(case when channel = 'web' then discount else 0 end) as web_discount,
    count(*) filter (where channel = 'store') as store_rows
from scored
group by case when prev_is_completed >= 83 then 'LARGE' else 'SMALL' end, case when shipping_fee >= 38 then 'PREMIUM' else 'STANDARD' end
order by store_rows;
