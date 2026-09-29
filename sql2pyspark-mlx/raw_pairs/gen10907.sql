with with_dims as (
    -- Attach products attributes
    select
        src.*,
        p.category,
        p.stock_qty
    from inventory src
    left join products p
        on src.product_id = p.id
),
scored as (
    -- Ranking and gap features
    select
        *,
        first_value(category) ignore nulls over (partition by month(last_restock), store_id order by last_restock asc, id asc rows between unbounded preceding and current row) as first_known_category,
        last_value(category, true) over (partition by product_id order by quantity asc, id asc rows between unbounded preceding and current row) as last_known_category,
        avg(quantity) over (partition by product_id order by stock_qty asc, id asc rows between 5 preceding and 1 preceding) as quantity_avg_5_prior
    from with_dims
),
joined as (
    -- Attach products attributes
    select
        e.id,
        e.store_id,
        e.product_id,
        e.stock_qty,
        e.first_known_category,
        p.product_name
    from scored e
    inner join products p
        on e.product_id = p.id
    where p.supplier_id is null
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        stock_qty,
        dense_rank() over (order by stock_qty asc nulls first) as stock_qty_dense_rank,
        row_number() over (partition by product_id order by stock_qty asc, id asc) as seq_no,
        last_value(stock_qty) ignore nulls over (partition by product_id order by stock_qty asc, id asc rows between unbounded preceding and current row) as last_known_stock_qty
    from joined
)
-- Aggregate with conditional metrics
select
    seq_no,
    count(*) as cnt,
    count(*) as event_count
from with_windows
group by seq_no;
