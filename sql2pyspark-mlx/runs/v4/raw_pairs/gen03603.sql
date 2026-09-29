select
    o.id,
    o.store_id,
    s.region,
    c.signup_date,
    datediff(current_date(), o.order_date) as days_since_order_date
from orders o
right join stores s on o.store_id = s.id
left join customers c on o.customer_id = c.id
where upper(trim(s.store_name)) = 'SIAM FLAGSHIP' or o.status in ('RETURNED', 'PENDING', 'CANCELLED');
