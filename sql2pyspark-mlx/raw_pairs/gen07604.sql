WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, INITCAP(TRIM(method)) method_title
    FROM payments
    WHERE paid_at >= TIMESTAMP '2023-06-01 12:00:00'
      AND amount < 1530
)
-- Window metrics over several different partitions
SELECT
    id,
    method,
    amount,
    DENSE_RANK() OVER (PARTITION BY method, status ORDER BY amount ASC) amount_dense_rank,
    LAST_VALUE(amount, TRUE) OVER (PARTITION BY order_id, status ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_amount,
    ROUND(amount / SUM(amount) OVER (PARTITION BY status), 4) amount_share
FROM base
