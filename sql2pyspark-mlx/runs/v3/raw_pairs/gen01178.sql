SELECT id, status, amount, ROUND(amount * 0.9, 2) amount_scaled FROM orders WHERE shipping_fee < 26 AND shipping_fee BETWEEN 17 AND 26;
