select
    id,
    customer_id,
    store_id,
    amount,
    shipping_fee,
    last_value(shipping_fee) over (partition by customer_id, store_id order by amount desc, id rows between unbounded preceding and unbounded following) as last_shipping_fee
from orders
where shipping_fee is not null;
