select
    payments.id,
    payments.method,
    orders.status,
    orders.customer_id,
    stores.city
from payments
join orders on payments.order_id = orders.id
right join stores on stores.id = orders.store_id;
