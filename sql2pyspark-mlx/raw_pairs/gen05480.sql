with base as (
    select
        id,
        product_name,
        category,
        brand,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        size(tags) as tags_count
    from products
    where brand is distinct from 'Stark'
),
metrics as (
    select
        id,
        product_name,
        brand,
        stock_qty,
        launch_date,
        supplier_id,
        ntile(4) over (partition by supplier_id, brand order by stock_qty, id) as stock_qty_bucket,
        ntile(3) over (partition by supplier_id order by cost desc, id) as cost_bucket
    from base
),
joined as (
    select src.*, b.city
    from metrics src
    left join suppliers b
        on src.supplier_id = b.id
),
scored as (
    select
        id,
        brand,
        stock_qty,
        launch_date,
        supplier_id,
        stock_qty_bucket,
        city,
        lag(cost_bucket) over w_ordered as prev_cost_bucket,
        lead(stock_qty_bucket) over w_ordered as next_stock_qty_bucket,
        lag(stock_qty_bucket) over w_ordered as prev_stock_qty_bucket,
        row_number() over w_ordered as row_seq
    from joined
    where length(product_name) > 6
    window w_ordered as (partition by city, brand order by cost_bucket, id)
)
select
    s.id,
    s.launch_date,
    s.supplier_id,
    s.stock_qty_bucket,
    s.city,
    s.prev_cost_bucket,
    s.next_stock_qty_bucket,
    (select min(x.cost) from products x where x.supplier_id = s.supplier_id) as min_product_cost
from scored s
order by supplier_id desc;
