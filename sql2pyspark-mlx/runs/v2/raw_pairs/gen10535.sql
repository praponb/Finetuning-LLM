select
    status,
    case when amount >= 1280 then 'high' when amount >= 470 then 'mid' else 'low' end amount_bucket,
    round(avg(amount), 2) avg_amount,
    count(amount) amount_count,
    max(paid_at) latest_paid_at
from payments
where method not in ('card', 'bank_transfer')
group by status, case when amount >= 1280 then 'high' when amount >= 470 then 'mid' else 'low' end
order by amount_bucket desc
