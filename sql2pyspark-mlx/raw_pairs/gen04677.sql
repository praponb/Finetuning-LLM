select
    case when amount >= 1690 then 'PREMIUM' else 'STANDARD' end as amount_tier,
    date_trunc('month', paid_at) as activity_month,
    percentile_approx(amount, 0.95) as amount_p95,
    (unix_timestamp(max(paid_at)) - unix_timestamp(min(paid_at))) / 60 as span_minutes,
    sum(case when method = 'promptpay' then amount else 0 end) as promptpay_amount
from payments
group by case when amount >= 1690 then 'PREMIUM' else 'STANDARD' end, date_trunc('month', paid_at)
having percentile_approx(amount, 0.95) < 1000000
order by amount_p95 desc nulls last;
