with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        order_id,
        product_id,
        quantity,
        unit_price * quantity as line_total,
        greatest(unit_price, quantity) as greatest_unit_price_quantity
    from order_items
    where order_id in (38, 40)
),
joined as (
    -- Attach products attributes
    select a.*, d.product_name
    from base a
    left join products d
        on a.product_id = d.id
    where d.price between 630 and 970
),
joined_order_items as (
    -- Attach products attributes
    select
        s.id,
        s.product_id,
        s.line_total,
        s.product_name,
        p.price,
        p.product_name as product_product_name,
        p.launch_date
    from joined s
    left join products p
        on s.product_id = p.id
),
enriched as (
    -- Attach products attributes
    select
        e.id,
        e.product_id,
        e.line_total,
        e.price,
        b.category,
        b.stock_qty,
        b.launch_date as product_launch_date
    from joined_order_items e
    left join products b
        on e.product_id = b.id
),
ranked as (
    -- QUALIFY-style window filter
    select
        id,
        product_id,
        line_total,
        category,
        stock_qty
    from (
        select *, row_number() over (partition by category order by stock_qty desc, id) as rn
        from enriched
    ) t
    where rn = 1
)
-- Running and moving aggregates
select
    id,
    product_id,
    line_total,
    category,
    row_number() over (order by line_total, id) as row_seq,
    row_number() over (partition by product_id order by stock_qty desc, id) as row_seq_2,
    avg(stock_qty) over (partition by category order by stock_qty, id rows between 3 preceding and 1 preceding) as stock_qty_avg_3_prior,
    sum(stock_qty) over w_hist as cumulative_stock_qty
from ranked
window w_hist as (order by stock_qty, id rows between unbounded preceding and current row)
