select date_format(s.opened_date, 'yyyy-MM') as opened_month, s.region, avg(o.amount) as avg_amount
from orders o
left join customers c on o.customer_id = c.id
join stores s on o.store_id = s.id
group by date_format(s.opened_date, 'yyyy-MM'), s.region;
