select discount, shipping_fee, amount - shipping_fee as amount_with_shipping from orders where amount < 1300 order by amount_with_shipping desc, discount
