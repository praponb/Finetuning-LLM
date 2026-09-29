WITH base AS (
    -- Keep recent, valid rows
    SELECT *, UPPER(status) AS status_upper
    FROM payments
    WHERE paid_at > TIMESTAMP '2023-09-11 10:00:00'
      AND method IS NOT NULL
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        order_id,
        amount,
        status,
        status_upper
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, method ORDER BY amount NULLS FIRST, id) AS rank_in_group
        FROM base
    ) x
    WHERE rank_in_group = 1
),
enriched AS (
    -- Attach orders attributes
    SELECT src.*, p.amount AS order_amount
    FROM deduped src
    LEFT JOIN orders p
        ON src.order_id <=> p.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        amount,
        order_amount,
        AVG(amount) OVER (ORDER BY amount, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS amount_avg_5,
        SUM(order_amount) OVER (ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_order_amount,
        AVG(order_amount) OVER (PARTITION BY order_id, status ORDER BY order_amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS order_amount_avg_6
    FROM enriched
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, amount + amount_avg_5 AS amount_plus_amount_avg_5
    FROM with_windows
    WHERE order_id IS NOT NULL
)
SELECT
    id,
    amount,
    order_amount,
    amount_avg_5,
    order_amount_avg_6,
    amount_plus_amount_avg_5
FROM cleaned
ORDER BY order_amount_avg_6 DESC
