select launch_date
from products
union all
select end_date as launch_date
from subscriptions
