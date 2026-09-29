with cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        least(unit_price, quantity) least_unit_price_quantity,
        round(quantity * 1.07, 2) adjusted_quantity
    from order_items
    where product_id is not null
      and unit_price > 180
),
enriched as (
    -- Ranking and gap features
    select
        id,
        order_id,
        quantity,
        adjusted_quantity,
        ntile(3) over (partition by order_id order by adjusted_quantity, id) adjusted_quantity_bucket,
        avg(quantity) over (partition by product_id, order_id order by unit_price, id rows between 5 preceding and 1 preceding) quantity_avg_5_prior
    from cleaned
    where order_id = 25
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by order_id order by adjusted_quantity_bucket desc nulls first, id) as dedup_rank
    from enriched
    qualify dedup_rank = 1
),
with_dims as (
    -- Attach orders attributes
    select
        src.*,
        d.shipping_fee,
        d.amount
    from deduped src
    left join orders d
        on src.order_id = d.id
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by order_id order by amount nulls first, id) as dedup_rank_2
        from with_dims
    ) t
    where dedup_rank_2 = 1
),
enriched_order_items as (
    -- Attach orders attributes
    select src.*, o.discount
    from latest src
    left join orders o
        on src.order_id = o.id
),
kept_keys as (
    -- Set intersection on keys
    select id
    from enriched_order_items
    intersect all
    select id
    from order_items
    where unit_price >= 260
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched_order_items s
    inner join kept_keys k on s.id = k.id
)
select
    s.id,
    s.order_id,
    s.quantity,
    s.adjusted_quantity,
    s.adjusted_quantity_bucket,
    s.dedup_rank,
    s.shipping_fee,
    s.dedup_rank_2,
    (select sum(t2.amount) from payments t2 where t2.order_id = s.order_id) as sum_payment_amount
from kept_rows s;
