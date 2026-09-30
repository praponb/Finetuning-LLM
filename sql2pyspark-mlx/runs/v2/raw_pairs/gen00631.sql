select round(avg(amount), 2) as avg_amount
from payments
where paid_at < '2024-05-01';
