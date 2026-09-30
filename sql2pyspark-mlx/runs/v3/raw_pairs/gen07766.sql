select id, shipping_fee, channel, coalesce(shipping_fee, 0) as shipping_fee_filled from orders where shipping_fee > 24 order by id desc;
