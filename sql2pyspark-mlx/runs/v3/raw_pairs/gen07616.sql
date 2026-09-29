SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY shipping_fee DESC, id) AS pos FROM orders) AS ranked WHERE pos <= 5 ORDER BY shipping_fee DESC
