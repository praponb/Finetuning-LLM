select o.status, round(stddev(oi.unit_price), 2) as stddev_unit_price
from order_items oi
full outer join orders o on oi.order_id = o.id
group by o.status
