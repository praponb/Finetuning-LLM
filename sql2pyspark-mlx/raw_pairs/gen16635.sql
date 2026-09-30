with matched_keys as (
    -- Set difference on keys
    select product_id, id
    from order_items
    except all
    select product_id, id
    from order_items
    where order_id = 8
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from order_items s
    inner join matched_keys k on s.product_id = k.product_id and s.id = k.id
)
select
    id,
    order_id,
    product_id,
    unit_price
from kept_rows
order by unit_price desc nulls first
