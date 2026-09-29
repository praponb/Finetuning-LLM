SELECT
    id,
    status,
    store_id,
    amount,
    NTILE(4) OVER (PARTITION BY status, store_id ORDER BY amount DESC, id) AS amount_ntile,
    ROUND(amount / SUM(amount) OVER (PARTITION BY status), 3) AS amount_share
FROM orders;
