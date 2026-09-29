with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        product_id,
        quantity,
        floor(unit_price) unit_price_whole
    from order_items
    where unit_price is not null
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        min(quantity) over w_part running_min_quantity,
        max(quantity) over w_part running_max_quantity,
        avg(quantity) over w_part running_avg_quantity,
        sum(quantity) over w_part running_sum_quantity
    from filtered
    where order_id in (4, 12, 32)
    window w_part as (partition by order_id order by unit_price_whole, id rows between unbounded preceding and current row)
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        last_value(running_avg_quantity) ignore nulls over (order by running_avg_quantity, id rows between unbounded preceding and current row) last_known_running_avg_quantity,
        dense_rank() over (partition by order_id order by unit_price_whole desc nulls first) unit_price_whole_dense_rank
    from enriched
),
joined as (
    -- Attach orders attributes
    select
        src.id,
        src.order_id,
        src.product_id,
        src.quantity,
        src.running_max_quantity,
        src.running_avg_quantity,
        src.running_sum_quantity,
        src.unit_price_whole_dense_rank,
        b.status,
        b.amount,
        b.order_date
    from with_windows src
    left join orders b
        on src.order_id = b.id
),
joined_order_items as (
    -- Attach orders attributes
    select
        src.id,
        src.order_id,
        src.quantity,
        src.running_max_quantity,
        src.running_sum_quantity,
        src.amount,
        o.amount as order_amount,
        o.shipping_fee
    from joined src
    inner join orders o
        on src.order_id = o.id
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        quantity,
        running_max_quantity,
        running_sum_quantity,
        amount,
        order_amount,
        count(running_max_quantity) over w_running order_id_count_running_max_quantity,
        ntile(5) over (partition by order_id order by running_sum_quantity, id) running_sum_quantity_bucket,
        sum(quantity) over w_recent running_quantity
    from joined_order_items
    window w_running as (partition by order_id), w_recent as (partition by order_id order by amount, id rows between unbounded preceding and current row)
),
base as (
    -- Keep recent, valid rows
    select
        id,
        quantity,
        running_sum_quantity,
        amount,
        order_amount,
        running_sum_quantity_bucket,
        running_quantity,
        round(quantity, 1) quantity_rounded,
        running_sum_quantity - running_sum_quantity_bucket running_sum_quantity_minus_running_sum_quantity_bucket
    from metrics
    where running_max_quantity between 3.4 and 4.6
),
report as (
    select
        id,
        quantity,
        running_sum_quantity,
        order_amount,
        running_sum_quantity_bucket,
        quantity_rounded,
        running_sum_quantity_minus_running_sum_quantity_bucket
    from base
    order by quantity_rounded nulls last, id
)
select
    id,
    running_sum_quantity,
    order_amount,
    running_sum_quantity_bucket,
    quantity_rounded,
    running_sum_quantity_minus_running_sum_quantity_bucket
from report;
