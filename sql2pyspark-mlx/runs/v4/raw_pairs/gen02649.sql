select
    orders.customer_id,
    orders.shipping_fee,
    customers.credit_limit,
    customers.age,
    stores.store_name,
    stores.opened_date
from orders
inner join customers on orders.customer_id = customers.id
left join stores on orders.store_id = stores.id
where lower(customers.email) like '%iam%'
order by orders.shipping_fee;
