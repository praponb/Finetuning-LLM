with product_summary as (
    select product_id, min(sale_date) min_sale_date, round(stddev(revenue), 2) stddev_revenue
    from store_sales
    group by product_id
)
select p.price, ps.min_sale_date, ps.stddev_revenue
from products p
inner join product_summary ps on p.id = ps.product_id
where ps.stddev_revenue > 64
