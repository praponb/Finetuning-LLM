select ss.id, ss.store_id, ss.revenue
from store_sales ss
where ss.revenue <= 1100 and ss.store_id in (select id from stores where city in ('Tokyo', 'Bangkok'))
order by revenue;
