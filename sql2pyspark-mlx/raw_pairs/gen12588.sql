WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_FORMAT(paid_at, 'yyyy-MM') AS paid_at_label
    FROM payments
    WHERE paid_at >= TO_TIMESTAMP('2023-10-26 05:00:00')
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        method,
        amount,
        paid_at,
        DENSE_RANK() OVER (ORDER BY amount DESC NULLS FIRST) AS amount_dense_rank,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY amount DESC NULLS LAST) AS amount_dense_rank_2,
        LAST_VALUE(method) IGNORE NULLS OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_method
    FROM cleaned
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount DESC, id) AS rank_in_group
        FROM enriched
    ) t
    WHERE rank_in_group = 1
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(amount_dense_rank_2, 2) AS amount_dense_rank_2_rounded,
        COALESCE(method, 'UNKNOWN') AS method_clean
    FROM latest
    WHERE HOUR(paid_at) BETWEEN 7 AND 14
)
SELECT
    id,
    method,
    amount,
    paid_at,
    amount_dense_rank,
    last_known_method,
    amount_dense_rank_2_rounded,
    method_clean,
    CASE WHEN method RLIKE '^(promptpay|bank_transfer)$' THEN 'GROUP_B' WHEN method RLIKE '^(card|paypal)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS method_zone
FROM base
ORDER BY amount_dense_rank_2_rounded DESC;
