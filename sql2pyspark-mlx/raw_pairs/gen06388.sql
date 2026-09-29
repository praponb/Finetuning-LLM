select order_id, amount, round(amount, 1) as amount_rounded
from payments
where amount between 950 and 1320 and to_date(paid_at) < '2023-08-13' and to_date(paid_at) >= '2023-06-01';
