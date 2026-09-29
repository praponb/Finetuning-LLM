with product_summary as (
    select product_id, sum(units) total_units, round(stddev(revenue), 2) stddev_revenue
    from store_sales
    group by product_id
)
select
    p.id,
    p.category,
    ps.total_units,
    ps.stddev_revenue
from products p
join product_summary ps on p.id = ps.product_id
order by total_units asc, p.id desc
limit 3
