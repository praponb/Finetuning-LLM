select store_id, stddev_discount, discount_count
from (
    select store_id, round(stddev(discount), 2) as stddev_discount, count(discount) as discount_count
    from orders
    where order_date between '2023-08-19' and '2024-06-01'
    group by store_id
) as x
where discount_count < 7
