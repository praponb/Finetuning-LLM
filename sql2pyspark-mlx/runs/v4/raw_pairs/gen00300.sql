SELECT id, channel, discount, shipping_fee FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount DESC, id) row_num FROM orders) r WHERE row_num <= 3 ORDER BY id, 3;
