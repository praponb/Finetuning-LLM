select date_trunc('MONTH', paid_at) as paid_month, sum(case when method = 'promptpay' then 1 else 0 end) as promptpay_count, count(distinct amount) as unique_orders
from payments
where hour(paid_at) between 11 and 14
group by date_trunc('MONTH', paid_at)
order by paid_month;
