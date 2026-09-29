with base as (
    -- Basic cleansing and defaults
    select
        id,
        category,
        brand,
        price,
        cost,
        coalesce(brand, 'unassigned') as brand_filled,
        substring(category, 1, 2) as category_prefix,
        coalesce(brand, 'OTHER') as resolved_brand
    from products
    where brand is distinct from 'Stark'
      and stock_qty <> 325
),
with_windows as (
    -- Ranking and gap features
    select
        id,
        category,
        brand_filled,
        sum(cost) over (partition by category, brand order by price, id rows between unbounded preceding and current row) as running_cost,
        sum(cost) over (order by cost, id rows between unbounded preceding and current row) as cumulative_cost,
        ntile(3) over (partition by category order by price, id) as price_bucket,
        ntile(5) over (order by cost desc, id) as cost_bucket,
        first_value(cost, true) over (partition by category order by price, id rows between unbounded preceding and current row) as first_known_cost
    from base
    where price between 350 and 850
),
latest_match as (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.category,
        s.cumulative_cost,
        s.price_bucket,
        s.cost_bucket,
        top1.last_restock
    from with_windows s
    left join lateral (
            select c.last_restock
            from inventory c
            where c.product_id = s.id
            order by c.last_restock desc, c.id desc
            limit 1
        ) top1 on true
),
with_latest as (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.inventory_last_restock,
        x.quantity
    from latest_match s
    join lateral (
            select c.last_restock as inventory_last_restock, c.quantity
            from inventory c
            where c.product_id = s.id
            order by c.quantity desc, c.id desc
            limit 1
        ) x on true
),
with_windows_products as (
    -- Running and moving aggregates
    select
        id,
        price_bucket,
        cost_bucket,
        last_restock,
        inventory_last_restock,
        quantity,
        min(cost_bucket) over w_recent as last7_min_cost_bucket,
        avg(cost_bucket) over w_recent as last7_avg_cost_bucket,
        count(cost_bucket) over w_recent as last7_count_cost_bucket
    from with_latest
    window w_recent as (partition by category order by last_restock, id rows between 6 preceding and current row)
),
with_windows_products_2 as (
    -- Window metrics over several different partitions
    select
        id,
        inventory_last_restock,
        quantity,
        last7_min_cost_bucket,
        rank() over (order by price_bucket nulls last) as price_bucket_rank,
        row_number() over (order by cost_bucket, id) as seq_no,
        avg(quantity) over (order by quantity, id rows between 6 preceding and current row) as quantity_avg_6
    from with_windows_products
)
-- Multi-level aggregation
select
    seq_no,
    round(avg(quantity_avg_6), 2) as avg_quantity_avg_6,
    round(avg(last7_min_cost_bucket), 2) as avg_last7_min_cost_bucket,
    count(*) as cnt,
    count(distinct id) as distinct_ids,
    count(*) as event_count
from with_windows_products_2
group by 1
having distinct_ids < 1000
order by avg_quantity_avg_6 nulls last;
