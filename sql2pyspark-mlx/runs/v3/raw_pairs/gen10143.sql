select
    id,
    shipping_fee,
    case when channel in ('web', 'mobile') then 'Tier 1' else 'Other' end as channel_group,
    discount - shipping_fee as discount_minus_shipping_fee
from orders
where order_date between '2023-05-03' and '2025-09-28' or discount > 0.33
