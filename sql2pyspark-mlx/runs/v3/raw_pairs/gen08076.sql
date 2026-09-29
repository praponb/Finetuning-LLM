select id, channel, amount, round(shipping_fee, 2) shipping_fee_rounded, amount - shipping_fee amount_with_shipping from orders where channel in ('partner', 'store');
