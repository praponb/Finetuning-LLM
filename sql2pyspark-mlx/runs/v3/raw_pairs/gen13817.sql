select c.country, max(o.shipping_fee) as max_shipping_fee, min(o.discount) as min_discount
from orders o
right join customers c on c.id = o.customer_id
where o.shipping_fee < 10
group by c.country;
