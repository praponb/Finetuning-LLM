select customers.country, max(accounts.balance) - min(accounts.balance) balance_range
from accounts
left outer join customers on accounts.customer_id = customers.id
group by customers.country
order by country
limit 5;
