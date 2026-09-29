SELECT id, discount, channel, shipping_fee FROM orders WHERE discount > (SELECT MAX(discount) FROM orders WHERE status IN ('CANCELLED', 'RETURNED'));
