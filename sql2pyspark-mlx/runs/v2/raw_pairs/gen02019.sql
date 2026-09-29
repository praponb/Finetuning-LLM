with accounts_by_customer as (
    select customer_id, count(*) as cnt
    from accounts
    where balance between 44000 and 71000
    group by customer_id
)
select c.name, c.segment, abc.cnt
from customers c
inner join accounts_by_customer abc on c.id = abc.customer_id;
