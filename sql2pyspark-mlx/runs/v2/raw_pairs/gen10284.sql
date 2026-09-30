SELECT
    id,
    channel,
    discount,
    NTILE(3) OVER (PARTITION BY channel ORDER BY discount, id) AS bucket
FROM orders;
