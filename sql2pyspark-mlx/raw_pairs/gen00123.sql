select round(avg(amount), 2) avg_amount, sum(case when status = 'SUCCESS' then 1 else 0 end) success_count
from payments
where to_date(paid_at) >= '2025-04-03' and method not in ('bank_transfer', 'card', 'promptpay')
order by success_count asc;
