select c.country, sum(case when a.account_type = 'LOAN' then 1 else 0 end) as loan_count
from accounts a
inner join customers c on a.customer_id = c.id
group by c.country
