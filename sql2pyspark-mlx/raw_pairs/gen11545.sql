select
    start_date,
    customer_id,
    plan,
    substring(status, 1, 3) as status_prefix,
    concat_ws(',', plan, status) as plan_status_key
from subscriptions
where year(end_date) >= 2025 and year(end_date) >= 2023
order by 1
