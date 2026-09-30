select status, total_discount, stddev_shipping_fee
from (
    select status, sum(discount) as total_discount, round(stddev(shipping_fee), 2) as stddev_shipping_fee
    from orders
    group by status
) as x
where total_discount >= 1.2
order by stddev_shipping_fee;
