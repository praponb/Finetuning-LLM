select year(s.opened_date) as opened_year, s.city, count(*) as num_store_sales
from store_sales ss
right join stores s on s.id = ss.store_id
group by year(s.opened_date), s.city
order by opened_year desc, city desc
