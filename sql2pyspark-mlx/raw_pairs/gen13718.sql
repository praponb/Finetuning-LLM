select status, 'payment' as origin
from payments
union all
select status, 'subscription' as origin
from subscriptions
where year(start_date) = 2024
order by status;
