select a.account_type, a.status, min(t.amount) as min_amount
from transactions t
inner join accounts a on t.account_id = a.id
inner join customers c on a.customer_id = c.id
where c.age <= 56
group by a.account_type, a.status
order by a.account_type, min_amount;
