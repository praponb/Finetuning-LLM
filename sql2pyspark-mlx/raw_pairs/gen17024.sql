select
    product_id,
    date_trunc('year', sale_date) as year_start,
    sum(revenue) as total_revenue,
    sum(revenue) as total_revenue_2,
    bit_and(units) as units_bit_and,
    approx_percentile(units, 0.9) as units_p90,
    stddev_samp(revenue) as revenue_stddev
from store_sales
group by 1, 2;
