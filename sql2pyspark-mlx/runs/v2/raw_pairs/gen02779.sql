select
    s.city,
    s.store_name,
    ss.product_id,
    ss.sale_date
from stores s
left join store_sales ss on ss.store_id = s.id
where s.region != 'North'
order by s.store_name;
