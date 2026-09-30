select id, monthly_fee, end_date
from subscriptions
where monthly_fee < (select min(monthly_fee) from subscriptions)
