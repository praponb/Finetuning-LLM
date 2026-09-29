select
    s.id,
    s.monthly_fee,
    s.status,
    s.plan,
    c.age
from subscriptions s
inner join customers c on c.id = s.customer_id
where upper(trim(c.name)) = 'PIM CHAIYAPORN' and c.signup_date between '2023-03-14' and '2025-06-18'
