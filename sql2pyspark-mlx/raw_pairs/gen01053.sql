WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM payments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at NULLS FIRST, id) = 1
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount DESC, id) = 1
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        method,
        amount,
        status
    FROM deduped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY method, order_id ORDER BY paid_at, id) = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(amount) OVER (PARTITION BY status ORDER BY amount, id) AS prev_amount,
        LEAD(amount) OVER (PARTITION BY status ORDER BY amount, id) AS next_amount,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount, id) AS seq_no,
        LAST_VALUE(method) IGNORE NULLS OVER (ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_method
    FROM latest
),
matched_keys AS (
    -- Set difference on keys
    SELECT status, id
    FROM metrics
    EXCEPT
    SELECT status, id
    FROM payments
    WHERE status NOT IN ('REFUNDED', 'SUCCESS')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN matched_keys k ON s.status = k.status AND s.id = k.id
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        method,
        status,
        prev_amount,
        next_amount,
        RANK() OVER (ORDER BY amount DESC) AS amount_rank,
        PERCENT_RANK() OVER w_hist AS prev_amount_percent_rank
    FROM reconciled
    WINDOW w_hist AS (PARTITION BY status ORDER BY prev_amount NULLS FIRST)
)
-- Grouped metrics
SELECT
    CASE WHEN amount_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_rank_tier,
    method,
    COUNT(*) AS event_count,
    PERCENTILE_APPROX(amount_rank, 0.9) AS amount_rank_p90,
    PERCENTILE_APPROX(prev_amount_percent_rank, 0.5) AS prev_amount_percent_rank_p50,
    PERCENTILE_APPROX(next_amount, ARRAY(0.25, 0.5, 0.75)) AS next_amount_quartiles
FROM enriched
GROUP BY 1, 2
HAVING event_count > 1
