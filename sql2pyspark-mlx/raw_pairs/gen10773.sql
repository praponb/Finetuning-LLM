WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        UPPER(status) AS status_upper
    FROM payments
    WHERE paid_at >= '2024-09-27'
),
enriched AS (
    -- Attach orders attributes
    SELECT
        a.*,
        p.amount AS order_amount,
        p.channel
    FROM filtered a
    LEFT JOIN orders p
        ON a.order_id <=> p.id
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        method,
        amount,
        channel
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY order_amount DESC NULLS FIRST, id) = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(amount) OVER (PARTITION BY channel, method ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_amount,
        MIN(amount) OVER (PARTITION BY channel, method ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_amount,
        AVG(amount) OVER (PARTITION BY channel, method ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_amount,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY amount DESC, id) AS event_seq
    FROM deduped
)
-- Multi-level aggregation
SELECT
    channel,
    COUNT(CASE WHEN channel = 'web' THEN 1 END) AS web_count,
    MIN(running_min_amount) AS min_running_min_amount,
    SORT_ARRAY(COLLECT_SET(method)) AS method_list,
    COUNT(DISTINCT method) AS distinct_methods,
    SUM(CASE WHEN method = 'promptpay' THEN amount ELSE 0 END) AS promptpay_amount
FROM with_windows
GROUP BY channel
ORDER BY min_running_min_amount
