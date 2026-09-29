select
    id,
    order_date,
    customer_id,
    shipping_fee
from orders
where year(order_date) >= 2025
