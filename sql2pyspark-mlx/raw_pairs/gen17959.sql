select c.segment, min(a.opened_date) as min_opened_date
from accounts a
left outer join customers c on a.customer_id = c.id
group by c.segment
order by min_opened_date
