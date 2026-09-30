select store_id, product_id, avg(revenue) as avg_revenue
from store_sales
where store_id in (2, 4, 6)
group by store_id, product_id
order by product_id desc;
