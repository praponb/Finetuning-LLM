select s.region, s.store_name, o.store_id
from stores s
left join orders o on s.id = o.store_id
where o.shipping_fee <= 31;
