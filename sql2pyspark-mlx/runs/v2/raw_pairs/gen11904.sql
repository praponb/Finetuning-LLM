select date_trunc('MONTH', order_date) order_month, max(discount) - min(discount) discount_range, max(order_date) last_order_date
from orders
where amount >= 940
group by 1
having count(*) > 5;
