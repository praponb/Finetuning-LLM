with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        order_id,
        quantity,
        coalesce(unit_price, 0) as unit_price_filled
    from order_items
    where product_id is not null
      and product_id = 8
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        order_id,
        quantity,
        unit_price_filled,
        rank() over (order by quantity asc) as quantity_rank,
        lag(unit_price_filled, 2) over (partition by order_id order by unit_price_filled asc, id asc) as prev_unit_price_filled,
        lead(quantity, 1) over w_part as next_quantity,
        ntile(5) over (partition by order_id order by unit_price_filled desc, id asc) as unit_price_filled_bucket
    from cleaned
    window w_part as (order by unit_price_filled asc, id asc)
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        sum(prev_unit_price_filled) over (order by unit_price_filled asc, id asc rows between unbounded preceding and current row) as cumulative_prev_unit_price_filled,
        lead(prev_unit_price_filled, 1, 0) over (partition by order_id order by quantity_rank asc, id asc) as next_prev_unit_price_filled
    from enriched
),
enriched_order_items as (
    -- Attach orders attributes
    select
        src.*,
        b.shipping_fee,
        b.amount
    from with_windows src
    inner join orders b
        on src.order_id = b.id
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from enriched_order_items
    qualify row_number() over (partition by order_id order by unit_price_filled_bucket asc, id asc) = 1
),
scored as (
    -- Running and moving aggregates
    select
        *,
        first_value(unit_price_filled) ignore nulls over (partition by order_id order by amount asc, id asc rows between unbounded preceding and current row) as first_known_unit_price_filled,
        lead(next_quantity, 2) over (order by cumulative_prev_unit_price_filled asc, id asc) as next_next_quantity,
        stddev_samp(amount) over (partition by order_id) as order_id_stddev_samp_amount,
        rank() over (partition by order_id order by unit_price_filled asc nulls first) as unit_price_filled_rank
    from deduped
)
select
    s.id,
    s.order_id,
    s.unit_price_filled,
    s.quantity_rank,
    s.prev_unit_price_filled,
    s.next_quantity,
    s.unit_price_filled_bucket,
    s.cumulative_prev_unit_price_filled,
    s.next_prev_unit_price_filled,
    (select max(x.quantity) from order_items x where x.order_id = s.order_id) as max_order_item_quantity
from scored s;
