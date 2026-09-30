with OrderTotals as (
    select order_id, sum(case when method = 'paypal' then amount else 0 end) paypal_amount
    from payments
    group by order_id
)
select o.shipping_fee, O.paypal_amount
from orders o
left join OrderTotals O on o.id = O.order_id
