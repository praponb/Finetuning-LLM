select customer_id, cast(shipping_fee as int) shipping_fee_int from orders where channel <> 'partner' and amount > 860 order by shipping_fee_int desc, customer_id;
