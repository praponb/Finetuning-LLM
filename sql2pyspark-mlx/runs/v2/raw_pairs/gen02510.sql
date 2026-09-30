select case when shipping_fee >= 32 then 'high' when shipping_fee >= 15 then 'mid' else 'low' end as shipping_fee_bucket, sum(amount) as total_amount, round(sum(amount) * 100.0 / sum(sum(amount)) over (), 2) as pct_of_total
from orders
where shipping_fee >= 12
group by case when shipping_fee >= 32 then 'high' when shipping_fee >= 15 then 'mid' else 'low' end;
