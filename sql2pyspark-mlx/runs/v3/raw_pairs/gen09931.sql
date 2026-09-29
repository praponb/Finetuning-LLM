select store_id, max(revenue) as max_revenue
from store_sales
where revenue is not null
group by 1
order by store_id desc;
