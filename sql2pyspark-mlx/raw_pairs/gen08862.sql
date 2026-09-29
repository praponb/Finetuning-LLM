with cleaned as (
    select
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        cost / nullif(stock_qty, 0) as cost_per_stock_qty,
        round(cost * 0.85, 2) as cost_scaled,
        coalesce(brand, 'NONE') as brand_filled
    from products
    where supplier_id is not null
),
enriched as (
    select
        e.id,
        e.product_name,
        e.brand,
        e.price,
        e.cost,
        e.stock_qty,
        e.tags,
        e.supplier_id,
        e.cost_scaled,
        e.brand_filled,
        p.city,
        p.country
    from cleaned e
    left join suppliers p
        on e.supplier_id = p.id
    where p.country = 'DE'
),
not_flagged as (
    select s.*
    from enriched s
    where exists (
            select 1
            from order_items c
            where c.product_id = s.id
              and c.unit_price between 150 and 230
        )
),
valid as (
    select
        *,
        array_join(tags, ', ') as tags_text,
        coalesce(brand, 'DIRECT') as brand_filled_2
    from not_flagged
    where brand is not null
      and country is null
)
select
    id,
    product_name,
    price,
    tags,
    supplier_id,
    city,
    country,
    ntile(3) over (partition by city order by stock_qty, id) as stock_qty_bucket,
    last_value(cost_scaled) ignore nulls over (partition by city order by stock_qty, id rows between unbounded preceding and current row) as last_known_cost_scaled
from valid
