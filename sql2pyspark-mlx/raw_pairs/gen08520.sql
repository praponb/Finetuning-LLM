select store_id, revenue_range, min_sale_date
from (
    select store_id, max(revenue) - min(revenue) as revenue_range, min(sale_date) as min_sale_date
    from store_sales
    group by store_id
) as s
where revenue_range > 63
order by store_id, min_sale_date desc
