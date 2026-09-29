with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        ceil(price) as price_whole,
        coalesce(brand, 'UNKNOWN') as brand_filled
    from products
    where launch_date >= to_date('2023-03-14')
      and brand = 'Initech'
),
metrics as (
    -- Ranking and gap features
    select
        *,
        avg(price_whole) over (partition by category, supplier_id order by cost, id rows between 5 preceding and current row) as price_whole_avg_5,
        avg(price_whole) over (partition by month(launch_date), brand order by cost, id rows between 5 preceding and current row) as price_whole_avg_5_2,
        percent_rank() over (partition by brand order by stock_qty desc) as stock_qty_percent_rank,
        ntile(10) over (partition by month(launch_date), brand order by price_whole, id) as price_whole_bucket
    from filtered
),
metrics_products as (
    -- Running and moving aggregates
    select
        id,
        category,
        brand,
        cost,
        supplier_id,
        price_whole,
        price_whole_avg_5,
        price_whole_avg_5_2,
        last_value(stock_qty, true) over (partition by brand, category order by price_whole_avg_5_2, id rows between unbounded preceding and current row) as last_known_stock_qty,
        lead(price_whole_avg_5, 1) over (order by price_whole_avg_5, id) as next_price_whole_avg_5,
        ntile(3) over (partition by supplier_id order by price desc, id) as price_bucket,
        sum(price) over (partition by brand order by price_whole_bucket, id rows between unbounded preceding and current row) as running_price,
        avg(stock_qty) over (partition by category order by price_whole_avg_5, id rows between 6 preceding and 1 preceding) as stock_qty_avg_6_prior
    from metrics
    where month(launch_date) in (2, 4, 5)
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        category,
        brand,
        supplier_id,
        next_price_whole_avg_5,
        price_bucket,
        running_price,
        stddev_samp(last_known_stock_qty) over (partition by supplier_id, brand) as supplier_id_stddev_samp_last_known_stock_qty,
        round(cost / sum(cost) over (partition by supplier_id, brand), 4) as cost_share,
        sum(price_bucket) over (partition by category order by price_bucket, id rows between unbounded preceding and current row) as running_price_bucket,
        cume_dist() over (partition by brand, category order by price_whole_avg_5 desc nulls first) as price_whole_avg_5_cume_dist
    from metrics_products
)
-- Join a dimension and aggregate
select
    dim.city,
    s.category,
    count(*) as rows_total,
    min(s.supplier_id_stddev_samp_last_known_stock_qty) as min_supplier_id_stddev_samp_last_known_stock_qty
from enriched s
inner join suppliers dim on s.supplier_id = dim.id
group by cube (dim.city, s.category)
order by min_supplier_id_stddev_samp_last_known_stock_qty nulls last;
