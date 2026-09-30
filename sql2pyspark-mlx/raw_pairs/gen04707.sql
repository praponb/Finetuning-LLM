with latest as (
    -- Keep one row per key (latest first)
    select *
    from (
        select *, row_number() over (partition by product_id, store_id order by last_restock desc, id) as rn
        from inventory
    ) t
    where rn = 1
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        product_id,
        last_restock,
        rn,
        rn + quantity as rn_plus_quantity,
        last_day(last_restock) as last_restock_month_end
    from latest
    where last_restock >= date '2023-07-21'
),
enriched as (
    -- Attach products attributes
    select
        a.*,
        p.product_name,
        p.category
    from filtered a
    left join products p
        on a.product_id = p.id
),
enriched_inventory as (
    -- Attach products attributes
    select
        a.*,
        d.brand,
        d.category as product_category,
        d.stock_qty
    from enriched a
    inner join products d
        on a.product_id = d.id
)
-- Grouped metrics
select
    case when rn_plus_quantity >= 76 then 'LARGE' else 'SMALL' end as rn_plus_quantity_tier,
    date_trunc('quarter', last_restock) as quarter_start,
    date_trunc('month', last_restock_month_end) as activity_month,
    round(sum(rn_plus_quantity) / nullif(count(case when brand = 'Initech' then 1 end), 0), 2) as rn_plus_quantity_per_initech,
    approx_percentile(rn_plus_quantity, array(0.25, 0.5, 0.75)) as rn_plus_quantity_quartiles,
    sum(case when category = 'Electronics' then stock_qty else 0 end) as electronics_stock_qty,
    count(distinct product_category) as distinct_product_categories,
    round(sum(rn_plus_quantity) / nullif(count(case when product_category = 'Sports' then 1 end), 0), 2) as rn_plus_quantity_per_sports,
    approx_percentile(stock_qty, array(0.25, 0.5, 0.75)) as stock_qty_quartiles,
    grouping(case when rn_plus_quantity >= 76 then 'LARGE' else 'SMALL' end) as gid
from enriched_inventory
group by cube (case when rn_plus_quantity >= 76 then 'LARGE' else 'SMALL' end, date_trunc('quarter', last_restock), date_trunc('month', last_restock_month_end))
having round(sum(rn_plus_quantity) / nullif(count(case when brand = 'Initech' then 1 end), 0), 2) > 0
