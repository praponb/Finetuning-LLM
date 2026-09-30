with cleaned as (
    select
        id,
        order_id,
        quantity,
        unit_price,
        coalesce(unit_price, 0) as unit_price_filled
    from order_items
    where product_id is not null
),
with_dims as (
    select
        s.*,
        d.shipping_fee,
        d.status,
        d.order_date
    from cleaned s
    join orders d
        on s.order_id = d.id
),
with_dims_order_items as (
    select
        a.id,
        a.order_id,
        a.quantity,
        a.unit_price,
        a.status,
        p.amount,
        p.order_date as order_order_date
    from with_dims a
    left join orders p
        on a.order_id = p.id
),
with_windows as (
    select
        id,
        order_id,
        quantity,
        status,
        amount,
        order_order_date,
        sum(amount) over (partition by order_id order by quantity, id rows between unbounded preceding and current row) as running_sum_amount,
        count(amount) over (partition by order_id order by quantity, id rows between unbounded preceding and current row) as running_count_amount,
        max(amount) over (partition by order_id order by quantity, id rows between unbounded preceding and current row) as running_max_amount,
        first_value(unit_price) ignore nulls over (partition by status order by quantity, id rows between unbounded preceding and current row) as first_known_unit_price
    from with_dims_order_items
),
enriched as (
    select
        *,
        ntile(3) over (partition by date_trunc('YEAR', order_order_date), order_id order by running_count_amount desc, id) as running_count_amount_bucket,
        row_number() over (order by running_max_amount, id) as row_seq,
        ntile(10) over w_ordered as running_count_amount_bucket_2
    from with_windows
    where running_max_amount > 490
    window w_ordered as (order by running_count_amount, id)
)
select
    id,
    order_id,
    metric_name,
    val
from (
    select
        id,
        order_id,
        running_max_amount,
        first_known_unit_price,
        running_sum_amount
    from enriched
) src
unpivot (
    val for metric_name in (running_max_amount as RUNNING_MAX_AMOUNT_VAL, first_known_unit_price as FIRST_KNOWN_UNIT_PRICE_VAL, running_sum_amount)
);
