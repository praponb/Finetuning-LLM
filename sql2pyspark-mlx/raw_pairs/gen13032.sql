SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY shipping_fee DESC, id) AS row_num
FROM orders
QUALIFY row_num = 1
