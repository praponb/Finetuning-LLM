select
    channel,
    discount,
    status,
    case when shipping_fee >= 34 then 'High' when shipping_fee >= 8 then 'Medium' else 'Low' end as shipping_fee_tier
from orders
where amount > 470 and shipping_fee is not null
order by shipping_fee_tier desc, discount;
