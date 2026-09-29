select date_trunc('MONTH', c.signup_date) as signup_month, avg(o.discount) as avg_discount, count(*) as num_orders
from orders o
join stores s on s.id = o.store_id
left join customers c on o.customer_id = c.id
where c.city = 'New York'
group by date_trunc('MONTH', c.signup_date)
order by num_orders;
