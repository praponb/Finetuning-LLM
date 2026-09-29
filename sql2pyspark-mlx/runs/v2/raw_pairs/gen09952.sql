select
    product_id,
    store_id,
    min(sale_date) as first_sale_date,
    round(stddev(revenue), 2) as stddev_revenue
from store_sales
where product_id in (1, 2)
group by 1, 2
having round(stddev(revenue), 2) > 43
order by first_sale_date
