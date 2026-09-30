select p.brand, count(ss.units) as units_count
from store_sales ss
left join products p on p.id = ss.product_id
inner join stores s on ss.store_id = s.id
group by p.brand
order by brand desc;
