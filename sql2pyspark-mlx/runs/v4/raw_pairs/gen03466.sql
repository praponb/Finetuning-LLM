select s.*, d.discount, d.channel, d.shipping_fee from shipments s left join orders d on s.order_id = d.id
