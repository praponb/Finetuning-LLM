with filtered as (
    -- Basic cleansing and defaults
    select
        *,
        stock_qty - price as stock_qty_minus_price,
        coalesce(brand, 'DIRECT') as brand_filled
    from products
    where launch_date <= '2024-10-01'
),
deduped as (
    -- QUALIFY-style window filter
    select *, row_number() over (partition by brand order by price, id) as rank_in_group
    from filtered
    qualify rank_in_group = 1
),
scored as (
    -- Running and moving aggregates
    select
        id,
        product_name,
        stock_qty,
        tags,
        brand_filled,
        ntile(3) over (partition by category, brand order by cost, id) as cost_bucket,
        lead(stock_qty, 1) over (partition by brand order by stock_qty_minus_price, id) as next_stock_qty,
        corr(price, cost) over (partition by year(launch_date), category) as corr_price_cost,
        ntile(3) over (partition by month(launch_date), category order by stock_qty, id) as stock_qty_bucket
    from deduped
    where category <> 'Books'
),
with_last as (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.stock_qty,
        s.tags,
        s.corr_price_cost,
        x.inventory_last_restock,
        x.inventory_quantity
    from scored s
    left join lateral (
            select c.last_restock as inventory_last_restock, c.quantity as inventory_quantity
            from inventory c
            where c.product_id = s.id
            order by c.last_restock desc, c.id desc
            limit 1
        ) x on true
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_last s
    where not exists (
            select 1
            from inventory c
            where c.product_id = s.id
              and c.store_id in (2, 3, 4)
        )
),
with_windows as (
    -- Running and moving aggregates
    select
        id,
        inventory_last_restock,
        inventory_quantity,
        max(corr_price_cost) over (order by inventory_quantity, id rows between unbounded preceding and current row) as running_max_corr_price_cost,
        sum(corr_price_cost) over (order by inventory_quantity, id rows between unbounded preceding and current row) as running_sum_corr_price_cost,
        avg(corr_price_cost) over (order by inventory_quantity, id rows between unbounded preceding and current row) as running_avg_corr_price_cost,
        lag(corr_price_cost, 2) over (partition by date_trunc('week', inventory_last_restock) order by inventory_quantity, id) as prev_corr_price_cost
    from kept
),
kept_products as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_windows s
    where not exists (
            select 1
            from store_sales x
            where x.product_id = s.id
              and x.sale_date >= s.inventory_last_restock
        )
)
select
    id,
    inventory_last_restock,
    running_max_corr_price_cost,
    running_sum_corr_price_cost,
    running_avg_corr_price_cost,
    prev_corr_price_cost
from kept_products
order by running_avg_corr_price_cost, id;
