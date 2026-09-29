SELECT id, status, discount FROM orders WHERE shipping_fee >= 34 OR status != 'COMPLETED';
