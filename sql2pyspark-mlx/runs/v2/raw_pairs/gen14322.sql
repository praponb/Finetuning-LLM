select month(s.opened_date) as opened_month, s.city, count(distinct ss.units) as unique_stores
from store_sales ss
left outer join products p on p.id = ss.product_id
inner join stores s on ss.store_id = s.id
where p.product_name is not null
group by month(s.opened_date), s.city
having count(distinct ss.units) > 3;
