select round(avg(amount), 2) as avg_amount, max(discount) - min(discount) as discount_range
from orders
order by avg_amount desc, discount_range desc;
