select case when s.rating >= 4.4 then 'high' when s.rating >= 2.3 then 'mid' else 'low' end as rating_bucket, s.city, avg(oi.unit_price) as avg_unit_price
from order_items oi
join products p on oi.product_id = p.id
right join suppliers s on p.supplier_id = s.id
group by case when s.rating >= 4.4 then 'high' when s.rating >= 2.3 then 'mid' else 'low' end, s.city
