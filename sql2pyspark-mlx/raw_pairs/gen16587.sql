select
    id,
    end_date,
    status,
    start_date
from subscriptions
where end_date = (select max(end_date) from subscriptions where status in ('EXPIRED', 'PAUSED'))
