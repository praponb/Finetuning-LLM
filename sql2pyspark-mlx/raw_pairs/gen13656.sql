with kept_keys as (
    -- Set intersection on keys
    select id, supplier_id
    from products
    intersect
    select id, supplier_id
    from products
    where supplier_id in (2, 15, 31, 33)
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from products s
    inner join kept_keys k on s.id = k.id and s.supplier_id = k.supplier_id
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        dense_rank() over (partition by category, supplier_id order by stock_qty nulls first) as stock_qty_dense_rank,
        corr(cost, stock_qty) over (partition by year(launch_date), category) as corr_cost_stock_qty
    from reconciled
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from enriched s
    where not exists (
            select 1
            from store_sales x
            where x.product_id = s.id
              and x.sale_date <= s.launch_date
        )
),
with_dims as (
    -- Attach suppliers attributes
    select
        s.id,
        s.product_name,
        s.category,
        s.brand,
        s.price,
        s.cost,
        s.stock_qty,
        s.launch_date,
        s.tags,
        s.supplier_id,
        s.corr_cost_stock_qty,
        d.city
    from not_flagged s
    left join suppliers d
        on s.supplier_id = d.id
),
metrics as (
    -- Ranking and gap features
    select
        id,
        category,
        cost,
        stock_qty,
        launch_date,
        supplier_id,
        corr_cost_stock_qty,
        dense_rank() over (order by corr_cost_stock_qty) as corr_cost_stock_qty_dense_rank,
        cume_dist() over (order by price desc nulls first) as price_cume_dist,
        dense_rank() over (order by stock_qty nulls last) as stock_qty_dense_rank
    from with_dims
),
joined as (
    -- Attach suppliers attributes
    select
        s.*,
        p.country,
        p.supplier_name,
        p.city
    from metrics s
    inner join suppliers p
        on s.supplier_id = p.id
    where p.rating is not null
),
metrics_products as (
    -- Window metrics over several different partitions
    select
        category,
        cost,
        stock_qty,
        launch_date,
        corr_cost_stock_qty,
        price_cume_dist,
        stock_qty_dense_rank,
        country,
        rank() over (partition by year(launch_date), category order by corr_cost_stock_qty_dense_rank desc nulls last) as corr_cost_stock_qty_dense_rank_rank,
        stddev_samp(corr_cost_stock_qty) over (partition by category, city) as category_stddev_samp_corr_cost_stock_qty,
        count(price_cume_dist) over (partition by city) as city_count_price_cume_dist,
        round(corr_cost_stock_qty_dense_rank / sum(corr_cost_stock_qty_dense_rank) over (partition by supplier_id), 4) as corr_cost_stock_qty_dense_rank_share,
        corr(stock_qty, cost) over w_recent as corr_stock_qty_cost
    from joined
    window w_recent as (partition by category)
)
-- Grouped metrics
select
    country,
    case when corr_cost_stock_qty >= 0.72 then 'HIGH' else 'LOW' end as corr_cost_stock_qty_tier,
    round(sum(stock_qty_dense_rank) / nullif(count(case when category = 'Electronics' then 1 end), 0), 2) as stock_qty_dense_rank_per_electronics,
    round(sum(corr_stock_qty_cost) / nullif(count(case when country = 'UK' then 1 end), 0), 2) as corr_stock_qty_cost_per_uk,
    count(*) as cnt
from metrics_products
group by grouping sets ((country, case when corr_cost_stock_qty >= 0.72 then 'HIGH' else 'LOW' end), (country))
having count(*) < 1000;
