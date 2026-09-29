select *
from (
    select date_trunc('MONTH', order_date) as order_month, sum(shipping_fee) as total_shipping_fee, min(shipping_fee) as min_shipping_fee
    from orders
    where discount between 0.2 and 0.36
    group by date_trunc('MONTH', order_date)
) as sub
where total_shipping_fee >= 130
order by min_shipping_fee, order_month;
