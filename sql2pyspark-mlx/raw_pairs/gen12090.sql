with metrics as (
    -- Window metrics over several different partitions
    select
        *,
        avg(unit_price) over (partition by product_id order by unit_price, id rows between unbounded preceding and current row) running_avg_unit_price,
        count(unit_price) over (partition by product_id order by unit_price, id rows between unbounded preceding and current row) running_count_unit_price,
        sum(unit_price) over (partition by product_id order by unit_price, id rows between unbounded preceding and current row) running_sum_unit_price,
        dense_rank() over (order by unit_price desc) unit_price_dense_rank
    from order_items
),
remaining_keys as (
    -- Set difference on keys
    select id, product_id
    from metrics
    except distinct
    select id, product_id
    from order_items
    where order_id = 13
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from metrics s
    inner join remaining_keys k on s.id = k.id and s.product_id = k.product_id
),
enriched as (
    -- Attach orders attributes
    select
        a.id,
        a.order_id,
        a.unit_price,
        a.running_avg_unit_price,
        a.running_count_unit_price,
        a.running_sum_unit_price,
        a.unit_price_dense_rank,
        o.status
    from kept_rows a
    inner join orders o
        on a.order_id = o.id
    where o.shipping_fee is null
),
kept_keys as (
    -- Set difference on keys
    select id, order_id
    from enriched
    except distinct
    select id, order_id
    from order_items
    where order_id = 35
),
kept_rows_order_items as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    inner join kept_keys k on s.id = k.id and s.order_id = k.order_id
),
with_dims as (
    -- Attach orders attributes
    select a.*, p.shipping_fee
    from kept_rows_order_items a
    inner join orders p
        on a.order_id = p.id
),
scored as (
    -- Ranking and gap features
    select *, rank() over (partition by order_id order by running_avg_unit_price desc) running_avg_unit_price_rank
    from with_dims
),
daily as (
    -- Aggregate with conditional metrics
    select
        case when shipping_fee >= 38 then 'PREMIUM' else 'STANDARD' end as shipping_fee_tier,
        approx_percentile(running_avg_unit_price, 0.95) as running_avg_unit_price_p95,
        max(running_avg_unit_price) as max_running_avg_unit_price,
        round(avg(shipping_fee), 2) as avg_shipping_fee
    from scored
    group by case when shipping_fee >= 38 then 'PREMIUM' else 'STANDARD' end
)
select
    shipping_fee_tier,
    running_avg_unit_price_p95,
    avg_shipping_fee
from daily
order by avg_shipping_fee desc
