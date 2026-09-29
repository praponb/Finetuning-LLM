select revenue
from store_sales
intersect
select monthly_fee revenue
from subscriptions
order by revenue
