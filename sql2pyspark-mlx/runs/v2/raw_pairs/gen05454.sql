select c.id, c.country, t.balance_range
from customers c
inner join (select customer_id, max(balance) - min(balance) as balance_range
    from accounts
    group by customer_id) t on c.id = t.customer_id
where t.balance_range >= 35
order by country desc, id asc
limit 5
