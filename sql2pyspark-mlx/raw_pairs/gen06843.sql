select
    id,
    country,
    customer_id,
    datediff(current_date(), started_at) / 365.25 as years_since_started_at
from sessions
where not (started_at >= '2025-04-12' and customer_id is not null)
order by years_since_started_at desc, 1 desc;
