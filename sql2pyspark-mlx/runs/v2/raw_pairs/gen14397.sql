select product_id, max(units) as max_units, max(revenue) - min(revenue) as revenue_range
from store_sales
where product_id = 3 and units between 28 and 29 and sale_date <= '2025-09-07'
group by product_id
order by revenue_range desc;
