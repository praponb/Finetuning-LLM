with valid as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        store_id,
        status,
        discount,
        shipping_fee,
        concat(channel, ' ', status) channel_status,
        coalesce(channel, 'OTHER') resolved_channel
    from orders
    where channel is distinct from 'web'
      and customer_id is not null
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        cume_dist() over (partition by status order by shipping_fee nulls last) shipping_fee_cume_dist,
        percent_rank() over (partition by customer_id order by discount desc) discount_percent_rank,
        ntile(4) over (order by discount, id) discount_bucket,
        ntile(4) over (partition by customer_id order by discount desc, id) discount_bucket_2
    from valid
),
joined as (
    -- Attach customers attributes
    select
        s.*,
        d.age,
        d.email
    from metrics s
    left join customers d
        on s.customer_id <=> d.id
),
metrics_orders as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        status,
        shipping_fee,
        shipping_fee_cume_dist,
        discount_percent_rank,
        age,
        email,
        avg(age) over w_running running_avg_age,
        min(age) over w_running running_min_age,
        sum(age) over w_running running_sum_age,
        round(discount_bucket_2 / sum(discount_bucket_2) over (partition by status, store_id), 4) discount_bucket_2_share
    from joined
    where status = 'SHIPPED'
    window w_running as (partition by customer_id, status order by discount_bucket, id rows between unbounded preceding and current row)
),
with_dims as (
    -- Attach customers attributes
    select
        s.*,
        d.city,
        d.country,
        d.segment
    from metrics_orders s
    left join customers d
        on s.customer_id = d.id
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_dims s
    where not exists (
            select 1
            from payments ch
            where ch.order_id = s.id
              and ch.amount > 540
        )
)
select
    s.id,
    s.customer_id,
    s.shipping_fee,
    s.discount_percent_rank,
    s.age,
    s.email,
    s.running_avg_age,
    s.running_min_age,
    s.running_sum_age,
    (select min(x.discount) from orders x where x.customer_id = s.customer_id) as min_order_discount
from kept s
order by customer_id desc, shipping_fee
