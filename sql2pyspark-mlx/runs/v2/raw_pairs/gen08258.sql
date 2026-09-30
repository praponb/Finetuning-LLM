select
    id,
    status,
    plan,
    end_date,
    concat(plan, ', ', status) as plan_status,
    add_months(start_date, 3) as start_date_plus_months
from subscriptions
