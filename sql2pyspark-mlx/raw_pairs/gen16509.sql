select status, date_trunc('MONTH', paid_at) as paid_month, count(amount) as amount_count
from payments
where order_id is null
group by status, date_trunc('MONTH', paid_at);
