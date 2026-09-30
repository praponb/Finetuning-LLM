WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipping_fee DESC, id) AS row_num FROM orders) SELECT id, status, shipping_fee, discount FROM numbered WHERE row_num <= 1
