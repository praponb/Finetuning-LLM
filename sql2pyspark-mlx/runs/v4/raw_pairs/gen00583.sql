with cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        round(cost, 1) as cost_rounded,
        cost / nullif(stock_qty, 0) as cost_per_stock_qty,
        coalesce(brand, 'UNKNOWN') as resolved_brand
    from products
    where launch_date >= date '2023-10-17'
),
matched_keys as (
    -- Set difference on keys
    select supplier_id, id
    from cleaned
    except distinct
    select supplier_id, id
    from products
    where supplier_id is null
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned s
    join matched_keys k on s.supplier_id = k.supplier_id and s.id = k.id
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        launch_date,
        cost_rounded,
        cost_per_stock_qty,
        resolved_brand,
        ceil(cost) as cost_whole,
        coalesce(brand, 'OTHER') as brand_filled
    from kept_rows
    where launch_date > to_date('2023-03-14')
      and brand is not null
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        launch_date,
        cost_rounded,
        cost_per_stock_qty,
        resolved_brand,
        cost_whole,
        brand_filled,
        percent_rank() over (order by cost_rounded desc nulls first) as cost_rounded_percent_rank,
        rank() over (partition by brand, category order by cost nulls first) as cost_rank,
        corr(cost, price) over (partition by brand, category) as corr_cost_price,
        percent_rank() over (order by cost_whole nulls last) as cost_whole_percent_rank
    from valid
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from metrics s
    where not exists (
            select 1
            from store_sales x
            where x.product_id = s.id
              and x.product_id in (1, 6, 7, 8)
        )
),
scored as (
    -- Ranking and gap features
    select
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        launch_date,
        cost_rounded,
        cost_per_stock_qty,
        resolved_brand,
        cost_whole,
        brand_filled,
        cost_rounded_percent_rank,
        cost_rank,
        corr_cost_price,
        cost_whole_percent_rank,
        avg(cost_whole_percent_rank) over (partition by brand, category) as brand_avg_cost_whole_percent_rank,
        percent_rank() over (partition by category order by cost_whole_percent_rank nulls first) as cost_whole_percent_rank_percent_rank
    from eligible
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        percent_rank() over (partition by category order by brand_avg_cost_whole_percent_rank desc nulls last) as brand_avg_cost_whole_percent_rank_percent_rank,
        max(cost_whole_percent_rank_percent_rank) over (partition by brand) as brand_max_cost_whole_percent_rank_percent_rank,
        cume_dist() over (partition by brand order by cost_rounded_percent_rank desc nulls last) as cost_rounded_percent_rank_cume_dist,
        dense_rank() over (partition by brand, category order by brand_avg_cost_whole_percent_rank desc nulls first) as brand_avg_cost_whole_percent_rank_dense_rank
    from scored
)
select
    id,
    product_name,
    category,
    brand,
    price,
    cost,
    launch_date,
    cost_rounded,
    cost_per_stock_qty
from enriched
order by brand nulls last;
