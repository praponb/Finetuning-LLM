select distinct plan, status
from subscriptions
where monthly_fee >= 200
