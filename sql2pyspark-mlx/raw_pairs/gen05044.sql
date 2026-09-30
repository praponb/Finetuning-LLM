with kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from orders s
    where not exists (
            select 1
            from order_items ch
            where ch.order_id = s.id
              and ch.product_id is null
        )
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        store_id,
        amount,
        discount,
        shipping_fee,
        count(discount) over w_running as running_count_discount,
        max(discount) over w_running as running_max_discount,
        sum(discount) over w_running as running_sum_discount,
        min(discount) over w_running as running_min_discount,
        sum(shipping_fee) over w_grp as cumulative_shipping_fee
    from kept
    where discount < 0.13
    window w_running as (partition by channel order by order_date asc, id asc rows between unbounded preceding and current row), w_grp as (partition by date_trunc('MONTH', order_date), customer_id order by shipping_fee asc, id asc rows between unbounded preceding and current row)
),
matched_keys as (
    -- Set difference on keys
    select id
    from metrics
    except distinct
    select id
    from orders
    where discount >= 0.1
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from metrics s
    inner join matched_keys k on s.id = k.id
),
valid as (
    -- Keep recent, valid rows
    select
        id,
        store_id,
        amount,
        discount,
        shipping_fee,
        running_sum_discount,
        cumulative_shipping_fee,
        case when running_sum_discount >= 1.99 then 'High' when running_sum_discount >= 0.88 then 'Medium' else 'Low' end as running_sum_discount_tier,
        case when shipping_fee >= 31 then 'High' when shipping_fee >= 18 then 'Medium' else 'Low' end as shipping_fee_level
    from verified
    where store_id in (21, 23)
),
summary as (
    -- Aggregate with conditional metrics
    select
        store_id,
        approx_percentile(cumulative_shipping_fee, array(0.25, 0.5, 0.75)) as cumulative_shipping_fee_quartiles,
        max(shipping_fee) as max_shipping_fee,
        avg(shipping_fee) as avg_shipping_fee,
        sum(cumulative_shipping_fee) as total_cumulative_shipping_fee,
        max(amount) as max_amount,
        approx_percentile(discount, 0.9) as discount_p90
    from valid
    group by store_id
)
select
    s.store_id,
    s.avg_shipping_fee,
    s.total_cumulative_shipping_fee,
    s.max_amount,
    s.discount_p90,
    (select sum(x.discount) from orders x where x.store_id = s.store_id) as sum_order_discount
from summary s
order by avg_shipping_fee desc nulls first
