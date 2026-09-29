with accounts_by_customer as (
    select customer_id, max(balance) as max_balance
    from accounts
    where account_type = 'LOAN'
    group by customer_id
)
select c.age, c.credit_limit, abc.max_balance
from customers c
inner join accounts_by_customer abc on c.id = abc.customer_id
where abc.max_balance > 48000
