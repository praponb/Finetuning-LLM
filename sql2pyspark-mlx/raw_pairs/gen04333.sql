WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        TRIM(method) AS clean_method
    FROM payments
    WHERE method IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        amount,
        clean_method,
        FIRST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY YEAR(paid_at), method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount,
        MAX(amount) OVER (PARTITION BY method) AS method_max_amount
    FROM valid
),
enriched_payments AS (
    -- Ranking and gap features
    SELECT
        id,
        amount,
        clean_method,
        first_known_amount,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount, id) AS event_seq,
        SUM(first_known_amount) OVER (PARTITION BY order_id ORDER BY first_known_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_first_known_amount,
        FIRST_VALUE(method_max_amount) IGNORE NULLS OVER (ORDER BY method_max_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_method_max_amount
    FROM enriched
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        amount,
        cumulative_first_known_amount,
        first_known_method_max_amount,
        SUM(amount) OVER (ORDER BY first_known_method_max_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        NTILE(4) OVER (ORDER BY event_seq, id) AS event_seq_bucket
    FROM enriched_payments
    WHERE first_known_method_max_amount >= 1171
),
report AS (
    SELECT
        id,
        amount,
        cumulative_first_known_amount
    FROM scored
)
SELECT
    id,
    amount,
    cumulative_first_known_amount
FROM report;
