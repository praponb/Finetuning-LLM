select
    id,
    end_date,
    start_date,
    status,
    date_add(end_date, 6) end_date_plus_6d,
    upper(status) status_upper
from subscriptions
where status = 'EXPIRED' or status not in ('ACTIVE', 'PAUSED', 'CANCELLED')
