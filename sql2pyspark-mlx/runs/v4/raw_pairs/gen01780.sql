with enriched as (
    select
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        cume_dist() over (order by revenue desc) as revenue_cume_dist,
        first_value(units, true) over w_grp as first_known_units
    from store_sales
    window w_grp as (partition by product_id order by sale_date asc, id asc rows between unbounded preceding and current row)
)
select
    date_trunc('week', sale_date) as week_start,
    min(revenue) as min_revenue,
    stddev_samp(revenue) as revenue_stddev,
    count(*) as num_rows,
    corr(units, revenue) as corr_units_revenue
from enriched
group by week_start
order by week_start asc;
