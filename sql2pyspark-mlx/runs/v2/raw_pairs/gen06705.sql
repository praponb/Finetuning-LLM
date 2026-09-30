select status, method, max(amount) as max_amount, count(*) as num_payments from payments group by status, method order by num_payments desc
