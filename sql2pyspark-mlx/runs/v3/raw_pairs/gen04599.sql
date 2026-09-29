with customer_stats as (
    select customer_id, max(balance) as max_balance, round(stddev(balance), 2) as stddev_balance
    from accounts
    where customer_id is null
    group by customer_id
)
select
    c.age,
    c.segment,
    cs.max_balance,
    cs.stddev_balance
from customers c
join customer_stats cs on c.id = cs.customer_id
where cs.stddev_balance > 36
