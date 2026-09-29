select c.segment, max(a.balance) - min(a.balance) as balance_range
from accounts a
inner join customers c on a.customer_id = c.id
where c.credit_limit > 15600
group by c.segment
order by balance_range desc
