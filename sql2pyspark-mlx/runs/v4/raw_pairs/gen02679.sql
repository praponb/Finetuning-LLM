select
    s.id,
    s.end_date,
    c.segment,
    upper(s.status) as status_upper
from subscriptions s
left outer join customers c on s.customer_id = c.id
where s.monthly_fee >= 190;
