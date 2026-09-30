select s.region, max(ss.revenue) max_revenue, count(ss.units) units_count
from store_sales ss
join stores s on ss.store_id = s.id
where s.city = 'Chiang Mai'
group by s.region;
