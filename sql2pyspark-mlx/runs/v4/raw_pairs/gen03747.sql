select o.status, round(stddev(oi.quantity), 2) as stddev_quantity, sum(oi.unit_price) as total_unit_price
from order_items oi
join orders o on oi.order_id = o.id
left join products p on oi.product_id = p.id
where o.amount >= 1530
group by o.status
order by total_unit_price;
