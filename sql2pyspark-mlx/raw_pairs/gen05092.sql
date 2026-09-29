select case when amount >= 1790 then 'high' when amount >= 300 then 'mid' else 'low' end as amount_bucket, avg(amount) as avg_amount, count(*) as cnt
from payments
group by case when amount >= 1790 then 'high' when amount >= 300 then 'mid' else 'low' end;
