SELECT
    id,
    channel,
    amount,
    RANK() OVER (PARTITION BY channel ORDER BY amount DESC) amount_rank
FROM orders;
