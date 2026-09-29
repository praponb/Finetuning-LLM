select cost
from products
union all
select monthly_fee
from subscriptions
where status not in ('ACTIVE', 'CANCELLED');
