select quarter(stores.opened_date) as opened_quarter, max(payments.amount) as max_amount, sum(payments.amount) as total_amount
from payments
inner join orders on payments.order_id = orders.id
left join stores on orders.store_id = stores.id
where stores.opened_date > '2024-05-01'
group by quarter(stores.opened_date)
