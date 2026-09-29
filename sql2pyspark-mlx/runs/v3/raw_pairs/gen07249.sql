with CustomerStats as (
    select customer_id, round(avg(balance), 2) avg_balance
    from accounts
    group by customer_id
)
select c.segment, C.avg_balance
from customers c
inner join CustomerStats C on c.id = C.customer_id
where C.avg_balance >= 29
order by C.avg_balance desc, c.segment;
