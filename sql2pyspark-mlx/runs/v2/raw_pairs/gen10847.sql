select *, coalesce(discount, 0) as discount_filled
from orders;
