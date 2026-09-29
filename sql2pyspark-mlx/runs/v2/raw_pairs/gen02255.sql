select orders.id, count(payments.id) as payment_count, avg(payments.amount) as avg_amount
from orders
left join payments on orders.id = payments.order_id
where payments.status not in ('REFUNDED', 'SUCCESS')
group by orders.id
order by id
