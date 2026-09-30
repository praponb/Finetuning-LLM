select c.segment, count(*) as row_count, min(a.balance) as min_balance
from accounts a
inner join customers c on c.id = a.customer_id
where a.opened_date between '2024-02-01' and '2024-12-01'
group by c.segment
having min(a.balance) > 58000
order by row_count;
