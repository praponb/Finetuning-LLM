select id, balance, customer_id
from accounts
where balance >= (select avg(balance) from accounts)
order by customer_id desc, id
limit 3
