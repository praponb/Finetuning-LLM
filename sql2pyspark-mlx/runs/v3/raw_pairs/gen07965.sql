select * from (select method, status, amount from payments where paid_at < '2025-01-01')
pivot (avg(amount) for status in ('SUCCESS', 'REFUNDED'))
order by method
