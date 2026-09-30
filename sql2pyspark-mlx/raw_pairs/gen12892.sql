select status
from subscriptions
except all
select status
from subscriptions
where monthly_fee < 240
