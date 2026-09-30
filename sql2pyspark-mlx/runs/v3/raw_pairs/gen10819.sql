select
    case when monthly_fee >= 206 then 'high' when monthly_fee >= 46 then 'mid' else 'low' end monthly_fee_bucket,
    status,
    min(monthly_fee) min_monthly_fee,
    max(monthly_fee) - min(monthly_fee) monthly_fee_range
from subscriptions
group by monthly_fee_bucket, status
having min(monthly_fee) < 150;
