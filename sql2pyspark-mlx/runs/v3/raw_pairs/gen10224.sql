select *, substring(plan, 1, 4) as plan_prefix, date_trunc('week', start_date) as start_date_period
from subscriptions
where monthly_fee between 220 and 230;
