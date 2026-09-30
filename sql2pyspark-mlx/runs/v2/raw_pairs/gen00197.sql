select id, customer_id, amount * shipping_fee as amount_with_shipping from orders where shipping_fee < 30 and discount between 0.11 and 0.18 order by amount_with_shipping desc
