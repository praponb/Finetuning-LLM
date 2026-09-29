select
    s.id,
    s.plan,
    s.end_date,
    s.monthly_fee,
    c.country,
    c.segment
from subscriptions s
left join customers c on s.customer_id = c.id
where s.monthly_fee > 80
order by s.id;
