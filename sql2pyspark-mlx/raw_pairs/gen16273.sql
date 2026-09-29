select c.is_active, sum(case when a.account_type = 'CREDIT' then 1 else 0 end) as credit_count
from accounts a
inner join customers c on a.customer_id = c.id
group by c.is_active
order by credit_count;
