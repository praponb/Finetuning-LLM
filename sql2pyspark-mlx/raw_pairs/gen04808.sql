SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY shipping_fee DESC, id) AS rank_in_group
FROM orders
QUALIFY rank_in_group = 1
