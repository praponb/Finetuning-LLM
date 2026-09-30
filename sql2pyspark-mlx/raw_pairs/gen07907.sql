with enriched as (
    select
        *,
        ntile(10) over (partition by product_id order by quantity asc, id asc) as quantity_bucket,
        rank() over (order by quantity desc nulls first) as quantity_rank
    from inventory
)
select
    date_trunc('week', last_restock) as week_start,
    avg(quantity) as avg_quantity,
    approx_percentile(quantity_bucket, array(0.25, 0.5, 0.75)) as quantity_bucket_quartiles,
    round(avg(quantity_bucket), 2) as avg_quantity_bucket,
    avg(quantity_bucket) as avg_quantity_bucket_2,
    round(avg(quantity_bucket), 2) as avg_quantity_bucket_3
from enriched
group by 1
