select status, count(amount) as amount_count, sum(amount) as total_amount from payments where status not in ('FAILED', 'SUCCESS', 'REFUNDED') group by status order by total_amount
