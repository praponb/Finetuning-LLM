select s.region, sum(ss.revenue) as total_revenue, min(ss.sale_date) as first_sale_date
from store_sales ss
inner join stores s on ss.store_id = s.id
where lower(trim(s.store_name)) = 'downtown express'
group by s.region
order by first_sale_date desc, region
limit 20
