with order_totals as (
    select order_id, round(stddev(amount), 2) stddev_amount
    from payments
    group by order_id
)
select o.id, o.shipping_fee, ot.stddev_amount
from orders o
left join order_totals ot on o.id = ot.order_id
where ot.stddev_amount >= 34
order by ot.stddev_amount desc
