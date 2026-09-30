select store_id, product_id, revenue_range
from (
    select store_id, product_id, max(revenue) - min(revenue) as revenue_range
    from store_sales
    where product_id in (4, 5, 9)
    group by store_id, product_id
) as t
where revenue_range < 51;
