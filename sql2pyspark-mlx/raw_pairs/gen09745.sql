select
    id,
    status,
    end_date,
    start_date,
    case when status = 'ACTIVE' then 1 else 0 end as is_active,
    concat(status, '/', plan) as status_plan
from subscriptions
