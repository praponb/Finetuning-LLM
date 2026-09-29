select amount as value
from orders
where amount is not null
except
select monthly_fee as value
from subscriptions
where monthly_fee is not null;
