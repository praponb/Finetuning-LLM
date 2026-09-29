with PaymentsByOrder as (
    select order_id, round((100 * sum(case when method = 'paypal' then 1 else 0 end)) / count(*), 1) as pct_paypal, count(*) as payment_count
    from payments
    where order_id = 14
    group by order_id
)
select o.shipping_fee, P.pct_paypal, P.payment_count
from orders o
left join PaymentsByOrder P on o.id = P.order_id
where P.pct_paypal >= 79
order by payment_count desc, shipping_fee desc;
