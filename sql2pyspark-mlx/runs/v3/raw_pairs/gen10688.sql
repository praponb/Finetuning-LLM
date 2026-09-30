select c.age, t.discount_count
from customers c
inner join (select customer_id, count(discount) as discount_count
    from orders
    where discount < 0.25
    group by customer_id) t on c.id = t.customer_id
where t.discount_count >= 6
