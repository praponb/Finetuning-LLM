select
    c.id,
    c.country,
    c.name,
    t.unique_customers
from customers c
inner join (select customer_id, count(distinct monthly_fee) as unique_customers
    from subscriptions
    group by customer_id) t on c.id = t.customer_id
where t.unique_customers >= 10
order by c.country, unique_customers
