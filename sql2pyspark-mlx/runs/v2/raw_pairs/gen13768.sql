select amount, lower(status) status_lower, cast(paid_at as date) paid_at_day
from payments
where method in ('card', 'bank_transfer', 'promptpay')
order by 2, paid_at_day
