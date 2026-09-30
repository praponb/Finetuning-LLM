select
    id,
    customer_id,
    discount,
    sum(discount) over (partition by month(order_date), status order by amount, id rows between unbounded preceding and current row) as running_discount,
    row_number() over (partition by store_id, status order by shipping_fee, id) as seq_no
from orders
