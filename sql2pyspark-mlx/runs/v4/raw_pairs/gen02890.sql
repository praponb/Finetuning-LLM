select
    method,
    round(stddev(amount), 2) as stddev_amount,
    count(*) as num_payments,
    sum(case when method = 'bank_transfer' then amount else 0 end) as bank_transfer_amount
from payments
where amount between 600 and 1300 and status in ('REFUNDED', 'SUCCESS') and order_id in (3, 7)
group by method
having sum(amount) > 7300
order by method desc;
