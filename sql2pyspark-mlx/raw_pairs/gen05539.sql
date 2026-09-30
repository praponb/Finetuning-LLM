select
    status,
    case when shipping_fee >= 33 then 'high' when shipping_fee >= 6 then 'mid' else 'low' end as shipping_fee_bucket,
    channel,
    count(case when status = 'COMPLETED' then 1 end) as completed_count,
    round(stddev(discount), 2) as stddev_discount
from orders
where amount between 650 and 1350
group by 1, 2, 3
having count(case when status = 'COMPLETED' then 1 end) < 5
