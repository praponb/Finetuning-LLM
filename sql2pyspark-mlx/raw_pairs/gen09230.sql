with base as (
    -- Keep recent, valid rows
    select *, coalesce(quantity, 0) as quantity_filled
    from order_items
    where order_id = 32
),
kept_keys as (
    -- Set intersection on keys
    select order_id
    from base
    intersect
    select order_id
    from order_items
    where unit_price >= 340
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from base s
    join kept_keys k on s.order_id = k.order_id
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        product_id,
        unit_price,
        quantity_filled,
        quantity + quantity_filled as quantity_plus_quantity_filled,
        unit_price - quantity as line_total
    from kept_rows
    where quantity between 6 and 8
),
with_dims as (
    -- Attach products attributes
    select
        a.id,
        a.product_id,
        a.unit_price,
        a.quantity_filled,
        p.stock_qty
    from filtered a
    left join products p
        on a.product_id = p.id
),
joined as (
    -- Attach products attributes
    select
        e.*,
        b.product_name,
        b.stock_qty as product_stock_qty
    from with_dims e
    join products b
        on e.product_id = b.id
)
select
    id,
    unit_price,
    stock_qty,
    product_name,
    product_stock_qty,
    case when product_name rlike '^(Smart Watch|Coffee Maker)$' then 'GROUP_A' when product_name rlike '^(Laptop Pro 14|wireless mouse)$' then 'PRIORITY' else 'CORE' end as product_name_zone
from joined
order by stock_qty desc;
