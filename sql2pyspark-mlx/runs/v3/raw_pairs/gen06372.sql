select month(p.launch_date) launch_month, avg(ss.revenue) avg_revenue
from store_sales ss
left outer join products p on ss.product_id = p.id
left outer join stores s on ss.store_id = s.id
group by month(p.launch_date)
order by launch_month asc
limit 20
