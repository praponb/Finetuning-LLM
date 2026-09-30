with accounts_by_customer as (
    select customer_id, max(balance) - min(balance) as balance_range
    from accounts
    where customer_id in (2, 13)
    group by customer_id
)
select customers.name, accounts_by_customer.balance_range
from customers
inner join accounts_by_customer on customers.id = accounts_by_customer.customer_id
where accounts_by_customer.balance_range > 71
order by name;
