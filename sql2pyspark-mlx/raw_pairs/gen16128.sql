with store_sales_by_product as (
    select product_id, min(revenue) as min_revenue, max(revenue) as max_revenue
    from store_sales
    where revenue <= 3100
    group by product_id
)
select
    p.id,
    p.stock_qty,
    ssbp.min_revenue,
    ssbp.max_revenue
from products p
left join store_sales_by_product ssbp on p.id = ssbp.product_id
order by ssbp.min_revenue desc, p.stock_qty;
