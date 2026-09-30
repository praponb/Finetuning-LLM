with with_windows as (
    -- Ranking and gap features
    select
        *,
        lag(unit_price) over w_hist as prev_unit_price,
        lead(unit_price) over w_hist as next_unit_price,
        lead(quantity) over w_hist as next_quantity,
        row_number() over w_hist as row_seq,
        min(quantity) over (partition by product_id) as product_id_min_quantity
    from order_items
    where product_id = 34
    window w_hist as (partition by order_id order by quantity asc, id asc)
)
select
    id,
    order_id,
    product_id,
    prev_unit_price,
    next_unit_price,
    next_quantity,
    product_id_min_quantity
from with_windows
order by product_id desc
