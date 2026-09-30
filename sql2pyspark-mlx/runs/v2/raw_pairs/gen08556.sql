select
    method,
    count(amount) amount_count,
    max(amount) max_amount,
    count(case when method = 'cash' then 1 end) cash_count
from payments
where order_id is not null and hour(paid_at) between 10 and 14
group by method
having max_amount >= 1310
order by method;
