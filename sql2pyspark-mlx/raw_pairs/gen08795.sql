select
    channel,
    sum(shipping_fee) as total_shipping_fee,
    count(*) as cnt,
    round(sum(shipping_fee) * 100.0 / sum(sum(shipping_fee)) over (), 2) as pct_of_total
from orders
where discount >= 0.34
group by channel;
