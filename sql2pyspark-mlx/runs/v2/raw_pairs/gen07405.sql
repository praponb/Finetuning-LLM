select
    orders.amount,
    customers.credit_limit,
    stores.city,
    stores.region
from orders
join customers on customers.id = orders.customer_id
left join stores on stores.id = orders.store_id
where stores.opened_date between '2023-09-01' and '2025-10-20' and orders.amount >= 1280;
