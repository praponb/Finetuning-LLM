select *, trim(status) as clean_status
from subscriptions
where plan = 'basic' and monthly_fee < 140
