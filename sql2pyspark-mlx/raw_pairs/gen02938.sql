with valid as (
    -- Keep recent, valid rows
    select
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        case when unit_price >= 430 then 'High' when unit_price >= 200 then 'Medium' else 'Low' end unit_price_band,
        floor(quantity) quantity_whole
    from order_items
    where unit_price < 240
),
scored as (
    -- Running and moving aggregates
    select
        *,
        row_number() over (partition by order_id, product_id order by quantity_whole desc, id) event_seq,
        lead(unit_price, 1) over (partition by order_id, product_id order by unit_price, id) next_unit_price,
        avg(quantity) over w_grp quantity_avg_5,
        row_number() over w_running row_seq
    from valid
    window w_grp as (order by quantity, id rows between 5 preceding and current row), w_running as (partition by order_id order by quantity desc, id)
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        product_id,
        unit_price,
        unit_price_band,
        quantity_whole,
        event_seq,
        next_unit_price,
        quantity_avg_5,
        row_seq,
        floor(event_seq) event_seq_whole,
        case when event_seq >= 7 then 'High' when event_seq >= 2 then 'Medium' else 'Low' end event_seq_band
    from scored
    where row_seq between 5 and 6
),
enriched as (
    -- Attach products attributes
    select
        a.*,
        p.category,
        p.price,
        p.cost
    from cleaned a
    left join products p
        on a.product_id = p.id
    where p.brand != 'Umbrella'
),
joined as (
    -- Attach products attributes
    select
        e.*,
        p.category as product_category,
        p.brand,
        p.product_name
    from enriched e
    left join products p
        on e.product_id = p.id
)
select
    id,
    unit_price,
    unit_price_band,
    event_seq,
    next_unit_price,
    quantity_avg_5,
    row_seq,
    event_seq_band,
    category
from joined
order by id desc
limit 10;
