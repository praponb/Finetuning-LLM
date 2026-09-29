select
    method,
    status,
    sum(case when method = 'paypal' then 1 else 0 end) as paypal_count,
    count(amount) as amount_count
from payments
where amount <= 1070 and method = 'bank_transfer'
group by method, status
having paypal_count < 52
order by method desc, status
limit 10
