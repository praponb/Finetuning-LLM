select
    id,
    store_id,
    revenue,
    round(revenue / units, 2) revenue_per_unit,
    date_trunc('YEAR', sale_date) sale_date_period
from store_sales
where product_id = 4 and revenue <= 1600
order by revenue_per_unit desc, id desc
limit 5
