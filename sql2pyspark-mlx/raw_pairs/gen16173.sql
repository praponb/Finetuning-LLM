select customers.is_active, avg(orders.discount) avg_discount, count(orders.shipping_fee) shipping_fee_count
from orders
full outer join stores on stores.id = orders.store_id
inner join customers on orders.customer_id = customers.id
where orders.status = 'RETURNED'
group by customers.is_active
order by avg_discount
