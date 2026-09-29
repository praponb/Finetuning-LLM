select start_date, ceil(monthly_fee) monthly_fee_whole
from subscriptions
where monthly_fee > 230
order by start_date desc, monthly_fee_whole desc
