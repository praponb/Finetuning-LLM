with with_latest as (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.city,
        s.rating,
        x.product_launch_date,
        x.product_stock_qty
    from suppliers s
    left join lateral (
            select c.launch_date as product_launch_date, c.stock_qty as product_stock_qty
            from products c
            where c.supplier_id = s.id
            order by c.launch_date desc, c.id desc
            limit 1
        ) x on true
)
-- Aggregate with conditional metrics
select
    date_trunc('week', product_launch_date) as week_start,
    max(product_stock_qty) filter (where city = 'Tokyo') as tokyo_max_product_stock_qty,
    size(collect_set(city)) as num_cities,
    approx_percentile(rating, array(0.25, 0.5, 0.75)) as rating_quartiles,
    count_if(city = 'Tokyo') as tokyo_events,
    count(*) as event_count,
    sum(case when city = 'London' then product_stock_qty else 0 end) as london_product_stock_qty
from with_latest
group by date_trunc('week', product_launch_date)
having size(collect_set(city)) < 1000
