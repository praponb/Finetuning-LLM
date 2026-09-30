with valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        product_id,
        quantity,
        unit_price,
        unit_price + quantity as line_total
    from order_items
    where product_id is not null
),
deduped as (
    -- Keep one row per key (latest first)
    select *
    from valid
    qualify row_number() over (partition by product_id order by quantity desc, id) = 1
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *
    from deduped
    qualify rank() over (partition by product_id order by unit_price nulls first) = 1
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        product_id,
        lag(unit_price) over w_part as prev_unit_price,
        lead(quantity) over w_part as next_quantity,
        row_number() over w_part as event_seq,
        avg(unit_price) over (order by quantity, id rows between 3 preceding and 1 preceding) as unit_price_avg_3_prior
    from latest
    where quantity < 4
    window w_part as (partition by product_id order by quantity, id)
),
latest_order_items as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by product_id order by unit_price_avg_3_prior, id) as latest_rank
    from enriched
    qualify latest_rank = 1
)
select
    id,
    product_id,
    next_quantity,
    unit_price_avg_3_prior,
    latest_rank
from latest_order_items
order by next_quantity nulls first;
