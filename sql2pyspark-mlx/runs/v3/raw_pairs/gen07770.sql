select
    orders.id,
    orders.status,
    orders.amount,
    stores.sqft,
    customers.country
from orders
left join stores on orders.store_id = stores.id
inner join customers on orders.customer_id = customers.id
where stores.region in ('South', 'East', 'West') and stores.opened_date > '2024-06-03'
order by orders.amount, id
limit 10;
