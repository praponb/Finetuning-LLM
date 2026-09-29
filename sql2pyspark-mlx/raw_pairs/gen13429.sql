WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        method,
        amount,
        status,
        TO_DATE(paid_at) AS paid_at_date,
        TRIM(status) AS clean_status
    FROM payments
    WHERE paid_at > TIMESTAMP '2023-06-07 16:00:00'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        order_id,
        method,
        amount,
        clean_status
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount DESC, id) AS row_num
        FROM cleaned
    ) t
    WHERE row_num = 1
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY amount DESC NULLS LAST) AS rn
        FROM deduped
    ) ranked
    WHERE rn = 1
),
matched_keys AS (
    -- Set difference on keys
    SELECT order_id, method
    FROM latest
    EXCEPT
    SELECT order_id, method
    FROM payments
    WHERE method IN ('card', 'promptpay')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    INNER JOIN matched_keys k ON s.order_id = k.order_id AND s.method = k.method
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        amount,
        clean_status,
        rn,
        ROUND(amount * 1.1, 2) AS adjusted_amount
    FROM verified
    WHERE amount BETWEEN 690 AND 1380
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        amount,
        adjusted_amount,
        DENSE_RANK() OVER (ORDER BY adjusted_amount DESC NULLS LAST) AS adjusted_amount_dense_rank
    FROM filtered
)
-- Multi-level aggregation
SELECT
    CASE WHEN adjusted_amount >= 82 THEN 'PREMIUM' ELSE 'STANDARD' END AS adjusted_amount_tier,
    CASE WHEN adjusted_amount_dense_rank >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS adjusted_amount_dense_rank_tier,
    ROUND(AVG(amount), 2) AS avg_amount,
    ROUND(AVG(adjusted_amount), 2) AS avg_adjusted_amount,
    ROUND(AVG(adjusted_amount), 2) AS avg_adjusted_amount_2
FROM enriched
GROUP BY CUBE (CASE WHEN adjusted_amount >= 82 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN adjusted_amount_dense_rank >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY adjusted_amount_dense_rank_tier DESC;
