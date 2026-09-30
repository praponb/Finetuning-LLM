select
    plan,
    sum(monthly_fee) as total_monthly_fee,
    count(*) as cnt,
    round(sum(monthly_fee) * 100.0 / sum(sum(monthly_fee)) over (), 2) as pct_of_total
from subscriptions
group by plan
order by 3 desc;
