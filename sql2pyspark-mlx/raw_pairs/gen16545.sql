select store_id, count(distinct revenue) as unique_stores, min(revenue) as min_revenue from store_sales group by store_id order by min_revenue desc
