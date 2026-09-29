select monthly_fee
from subscriptions
where monthly_fee is not null
intersect
select cost
from products
where cost is not null;
