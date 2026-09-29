select
    c.id,
    c.signup_date,
    c.country,
    t.avg_discount
from customers c
inner join (select customer_id, round(avg(discount), 2) as avg_discount
    from orders
    group by customer_id) t on c.id = t.customer_id
where t.avg_discount >= 59
