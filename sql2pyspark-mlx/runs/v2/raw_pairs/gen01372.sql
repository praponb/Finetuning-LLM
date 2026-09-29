select c.segment, sum(a.balance) as total_balance
from accounts a
join customers c on c.id = a.customer_id
group by c.segment
having sum(a.balance) > 121000
order by c.segment, total_balance desc;
