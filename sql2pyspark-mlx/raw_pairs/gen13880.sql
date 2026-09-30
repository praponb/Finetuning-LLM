with filtered as (
    -- Keep recent, valid rows
    select
        id,
        product_name,
        brand,
        price,
        stock_qty,
        launch_date,
        tags,
        coalesce(brand, 'N/A') as brand_filled
    from products
    where brand is not null
),
latest_match as (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    select
        s.*,
        latest.store_sale_revenue,
        latest.sale_date
    from filtered s
    left join lateral (
            select c.revenue as store_sale_revenue, c.sale_date
            from store_sales c
            where c.product_id = s.id
              and c.sale_date <= s.launch_date
            order by c.sale_date desc, c.id desc
            limit 1
        ) latest on true
),
deduped as (
    -- QUALIFY-style window filter
    select
        id,
        brand,
        price,
        stock_qty,
        tags
    from latest_match
    qualify row_number() over (partition by brand order by price desc, id) = 1
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        tags,
        lead(stock_qty, 1, 0) over w_recent as next_stock_qty,
        lead(stock_qty, 2, 0) over w_recent as next_stock_qty_2
    from deduped
    where array_contains(tags, 'bestseller')
    window w_recent as (partition by brand order by stock_qty, id)
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from enriched s
    where not exists (
            select 1
            from order_items c
            where c.product_id = s.id
              and c.order_id in (11, 18, 26)
        )
)
select
    id,
    tags,
    next_stock_qty,
    next_stock_qty_2
from not_flagged
order by id nulls last;
