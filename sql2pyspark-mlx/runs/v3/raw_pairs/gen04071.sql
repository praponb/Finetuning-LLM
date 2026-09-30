select year(paid_at) as paid_year, round(avg(amount), 2) as avg_amount
from payments
where status in ('REFUNDED', 'SUCCESS')
group by year(paid_at)
order by avg_amount, paid_year
limit 10;
