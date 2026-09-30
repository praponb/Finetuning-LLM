select
    s.id,
    s.monthly_fee,
    c.country,
    date_trunc('MONTH', s.end_date) as end_date_period
from subscriptions s
left join customers c on c.id = s.customer_id
where c.name like '%Mia%'
order by id desc, end_date_period
