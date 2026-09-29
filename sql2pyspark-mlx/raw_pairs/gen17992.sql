select date_trunc('MONTH', s.opened_date) as opened_month, round(avg(o.amount), 2) as avg_amount, count(case when o.status = 'RETURNED' then 1 end) as returned_count
from orders o
full outer join stores s on o.store_id = s.id
group by date_trunc('MONTH', s.opened_date)
order by returned_count desc, avg_amount desc;
