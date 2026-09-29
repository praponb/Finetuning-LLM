select
    id,
    customer_id,
    order_date,
    discount,
    lead(discount, 2) over (partition by customer_id order by order_date, id) as next_discount
from orders;
