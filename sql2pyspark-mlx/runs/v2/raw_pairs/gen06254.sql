select
    id,
    monthly_fee,
    end_date,
    start_date
from subscriptions
where status = 'ACTIVE' and month(end_date) in (3, 6, 8) and status = 'EXPIRED'
order by monthly_fee desc;
