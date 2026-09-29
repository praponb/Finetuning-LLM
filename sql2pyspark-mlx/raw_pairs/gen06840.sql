with cleaned as (
    -- Filter the raw rows and derive helper columns
    select *, quantity - unit_price line_total
    from order_items
    where product_id is not null
),
matched_keys as (
    -- Set difference on keys
    select product_id
    from cleaned
    except distinct
    select product_id
    from order_items
    where quantity = 6
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned s
    inner join matched_keys k on s.product_id = k.product_id
),
enriched as (
    -- Attach products attributes
    select
        s.product_id,
        s.quantity,
        s.unit_price,
        s.line_total,
        b.product_name,
        b.launch_date,
        b.brand
    from verified s
    left join products b
        on s.product_id = b.id
),
joined as (
    -- Attach products attributes
    select
        s.product_id,
        s.quantity,
        s.product_name,
        s.launch_date,
        p.stock_qty
    from enriched s
    left join products p
        on s.product_id = p.id
)
select
    s.product_id,
    s.quantity,
    s.launch_date,
    (select avg(s2.units) from store_sales s2 where s2.product_id = s.product_id) as avg_store_sale_units
from joined s
order by launch_date desc
