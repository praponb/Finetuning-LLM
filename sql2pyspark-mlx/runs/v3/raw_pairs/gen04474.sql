select *
from (
    select status, max(shipping_fee) as max_shipping_fee, max(order_date) as latest_order_date
    from orders
    group by status
) as x
where max_shipping_fee > 11
