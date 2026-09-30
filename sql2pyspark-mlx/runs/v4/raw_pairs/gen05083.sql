SELECT
    *,
    SUM(amount) OVER (ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_sum_30d,
    ROW_NUMBER() OVER (ORDER BY amount, id) AS row_seq,
    ROUND(amount / SUM(amount) OVER (PARTITION BY method), 4) AS amount_share
FROM payments;
