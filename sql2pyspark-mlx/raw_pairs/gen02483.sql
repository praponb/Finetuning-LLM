select
    s.delivered_date,
    o.channel,
    o.discount,
    o.shipping_fee
from shipments s
join orders o on s.order_id = o.id
where s.shipped_date <= '2024-10-01' and o.shipping_fee > 11;
