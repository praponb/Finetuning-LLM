select accounts.status, round(stddev(transactions.amount), 2) stddev_amount
from transactions
left join accounts on transactions.account_id = accounts.id
right join customers on accounts.customer_id = customers.id
where transactions.is_flagged
group by accounts.status
having round(stddev(transactions.amount), 2) > 61
order by accounts.status asc
limit 3
