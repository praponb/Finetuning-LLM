select
    product_id,
    year(last_restock) as last_restock_year,
    min(quantity) as min_quantity,
    round(stddev(quantity), 2) as stddev_quantity,
    round(avg(quantity), 2) as avg_quantity
from inventory
where product_id in (1, 5)
group by rollup(product_id, year(last_restock))
order by stddev_quantity desc, last_restock_year desc
