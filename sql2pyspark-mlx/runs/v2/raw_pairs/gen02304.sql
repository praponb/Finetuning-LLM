select case when p.price >= 1240 then 'high' when p.price >= 510 then 'mid' else 'low' end as price_bucket, avg(oi.quantity) as avg_quantity, max(oi.unit_price) as max_unit_price
from order_items oi
join products p on oi.product_id = p.id
join suppliers s on p.supplier_id = s.id
where s.city = 'Bangkok'
group by case when p.price >= 1240 then 'high' when p.price >= 510 then 'mid' else 'low' end
order by max_unit_price
