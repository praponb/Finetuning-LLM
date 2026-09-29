with Src as (
    select *
    from orders
    where channel in ('store', 'mobile') or discount < 0.14
),
orders_stats as (
    select status, round(avg(shipping_fee), 2) avg_shipping_fee, sum(case when channel = 'store' then discount else 0 end) store_discount
    from Src
    group by status
)
select status, avg_shipping_fee
from orders_stats
order by avg_shipping_fee desc;
