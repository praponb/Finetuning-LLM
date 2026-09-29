select status, count(distinct shipping_fee) as unique_stores, max(discount) - min(discount) as discount_range
from orders
group by status
