select customers.country, count(subscriptions.id) as subscription_count, sum(subscriptions.monthly_fee) as total_monthly_fee
from customers
inner join subscriptions on subscriptions.customer_id = customers.id
where customers.age > 32
group by customers.country
order by customers.country, subscription_count desc;
