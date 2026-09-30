select p.brand, round(stddev(oi.quantity), 2) stddev_quantity, avg(oi.quantity) avg_quantity
from order_items oi
right join products p on oi.product_id = p.id
left join orders o on oi.order_id = o.id
group by p.brand
