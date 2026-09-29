select
    id,
    customer_id,
    monthly_fee,
    start_date,
    case when plan in ('enterprise', 'premium') then 'Core' else 'Other' end as plan_group
from subscriptions;
