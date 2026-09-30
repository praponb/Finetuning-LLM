with valid as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        case when method = 'cash' then 1 else 0 end as is_cash,
        coalesce(method, 'N/A') as method_clean
    from payments
    where paid_at > date '2023-10-20'
),
latest as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by status order by is_cash nulls first, id) as row_num
    from valid
    qualify row_num = 1
),
with_dims as (
    -- Attach orders attributes
    select
        e.id,
        e.order_id,
        e.method,
        e.paid_at,
        e.status,
        e.row_num,
        o.status as order_status,
        o.order_date
    from latest e
    left join orders o
        on e.order_id = o.id
    where o.amount >= 480
),
enriched as (
    -- Attach orders attributes
    select
        a.id,
        a.order_id,
        a.status,
        a.order_status,
        o.shipping_fee
    from with_dims a
    inner join orders o
        on a.order_id = o.id
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        shipping_fee,
        coalesce(order_status, 'N/A') as order_status_filled,
        initcap(trim(order_status)) as order_status_title,
        coalesce(order_status, 'NONE') as resolved_order_status
    from enriched
    where order_id is not null
),
scored as (
    -- Window metrics over several different partitions
    select
        *,
        last_value(shipping_fee, true) over (order by shipping_fee, id rows between unbounded preceding and current row) as last_known_shipping_fee,
        avg(shipping_fee) over (order by shipping_fee, id rows between 5 preceding and current row) as shipping_fee_avg_5,
        cume_dist() over w_grp as shipping_fee_cume_dist,
        ntile(10) over (order by shipping_fee desc, id) as shipping_fee_bucket
    from cleaned
    window w_grp as (order by shipping_fee nulls last)
)
-- Aggregate with conditional metrics
select
    case when shipping_fee >= 44 then 'HIGH' else 'LOW' end as shipping_fee_tier,
    case when shipping_fee_cume_dist >= 0.67 then 'PREMIUM' else 'STANDARD' end as shipping_fee_cume_dist_tier,
    max(last_known_shipping_fee) as max_last_known_shipping_fee,
    count(distinct id) as distinct_ids,
    approx_percentile(shipping_fee, array(0.25, 0.5, 0.75)) as shipping_fee_quartiles,
    round(avg(shipping_fee_cume_dist), 2) as avg_shipping_fee_cume_dist,
    avg(shipping_fee_cume_dist) as avg_shipping_fee_cume_dist_2,
    count(*) as event_count,
    rank() over (partition by case when shipping_fee >= 44 then 'HIGH' else 'LOW' end order by max(last_known_shipping_fee) desc) as max_last_known_shipping_fee_rank
from scored
group by 1, 2
having avg(shipping_fee_cume_dist) > 0
order by shipping_fee_tier nulls last
