select orders.order_date, payments.paid_at, lower(payments.status) as status_lower from orders left join payments on payments.order_id = orders.id
