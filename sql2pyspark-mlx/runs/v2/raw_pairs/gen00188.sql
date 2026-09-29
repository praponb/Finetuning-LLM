select c.segment, count(distinct o.shipping_fee) as unique_channels
from orders o
left join customers c on o.customer_id = c.id
group by c.segment
having count(distinct o.shipping_fee) > 4;
