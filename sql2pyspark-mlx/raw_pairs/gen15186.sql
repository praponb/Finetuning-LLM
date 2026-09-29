with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        store_name,
        opened_date,
        sqft,
        concat_ws('-', store_name, region) as store_name_region_key,
        upper(region) as region_upper
    from stores
    where opened_date >= to_date('2023-07-21')
      and region is not null
),
with_latest as (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.store_name,
        latest.revenue,
        latest.units
    from filtered s
    join lateral (
            select c.revenue, c.units
            from store_sales c
            where c.store_id = s.id
            order by c.units desc, c.id desc
            limit 1
        ) latest on true
),
latest_match as (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.store_name,
        top1.last_restock,
        top1.quantity
    from with_latest s
    left join lateral (
            select c.last_restock, c.quantity
            from inventory c
            where c.store_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) top1 on true
),
base as (
    -- Filter the raw rows and derive helper columns
    select *, length(store_name) as store_name_length
    from latest_match
    where quantity <= 140
),
kept as (
    -- Semi-join via a correlated subquery
    select s.*
    from base s
    where exists (
            select 1
            from orders ch
            where ch.store_id = s.id
              and ch.order_date >= s.last_restock
        )
),
grouped as (
    -- Multi-level aggregation
    select
        date_trunc('week', last_restock) as week_start,
        case when quantity >= 219 then 'PREMIUM' else 'STANDARD' end as quantity_tier,
        round(avg(quantity), 2) as avg_quantity,
        percentile_approx(store_name_length, 0.9) as store_name_length_p90
    from kept
    group by 1, 2
)
select
    week_start,
    quantity_tier,
    store_name_length_p90
from grouped
order by quantity_tier desc
