select status, 'payment' as source
from payments
where method in ('cash', 'bank_transfer')
union
select status, 'subscription' as source
from subscriptions
where start_date between '2023-03-01' and '2023-04-01'
order by source;
