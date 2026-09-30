SELECT
    id,
    channel,
    shipping_fee,
    store_id
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY shipping_fee DESC, id) AS row_num
    FROM orders
) AS ranked
WHERE row_num <= 3;
