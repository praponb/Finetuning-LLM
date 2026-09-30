select a.status, round(avg(t.amount), 2) as avg_amount
from transactions t
inner join accounts a on t.account_id = a.id
left join customers c on a.customer_id = c.id
group by a.status;
