select s.city, max(ss.revenue) as max_revenue
from store_sales ss
full outer join stores s on ss.store_id = s.id
inner join products p on p.id = ss.product_id
where ss.units <= 27
group by s.city
order by max_revenue;
