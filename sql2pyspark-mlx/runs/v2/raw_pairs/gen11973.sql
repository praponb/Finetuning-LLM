select case when monthly_fee >= 236 then 'high' when monthly_fee >= 43 then 'mid' else 'low' end as monthly_fee_bucket, count(monthly_fee) as monthly_fee_count
from subscriptions
where (month(end_date) in (9, 10, 11) and status in ('EXPIRED', 'CANCELLED')) or month(end_date) in (5, 6, 11)
group by case when monthly_fee >= 236 then 'high' when monthly_fee >= 43 then 'mid' else 'low' end
having monthly_fee_count < 4
order by monthly_fee_bucket;
