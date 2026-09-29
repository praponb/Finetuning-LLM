WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        method,
        amount,
        CASE WHEN status IN ('REFUNDED', 'FAILED') THEN 'Core' ELSE 'Other' END AS status_group,
        CASE WHEN amount >= 1240 THEN 'High' WHEN amount >= 400 THEN 'Medium' ELSE 'Low' END AS amount_tier
    FROM payments
    WHERE paid_at >= '2023-05-23'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount DESC NULLS LAST, id) AS latest_rank
    FROM valid
    QUALIFY latest_rank = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY latest_rank DESC, id) AS rn
        FROM latest
    ) t
    WHERE rn = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(amount) OVER (PARTITION BY method ORDER BY latest_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_amount,
        MAX(amount) OVER (PARTITION BY method ORDER BY latest_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_amount,
        COUNT(amount) OVER (PARTITION BY method ORDER BY latest_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_amount,
        MIN(amount) OVER (PARTITION BY method ORDER BY latest_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_amount,
        CUME_DIST() OVER (PARTITION BY order_id ORDER BY amount DESC) AS amount_cume_dist
    FROM ranked
    WHERE order_id IS NULL
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN running_max_amount >= 1480 THEN 'High' WHEN running_max_amount >= 210 THEN 'Medium' ELSE 'Low' END AS running_max_amount_level,
        COALESCE(method, 'N/A') AS resolved_method
    FROM metrics
    WHERE method IS NOT NULL
),
ranked_payments AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        order_id,
        amount,
        latest_rank,
        running_sum_amount,
        running_count_amount,
        amount_cume_dist,
        running_max_amount_level,
        resolved_method
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY latest_rank DESC, id) AS rn_2
        FROM cleaned
    ) t
    WHERE rn_2 = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(running_count_amount, 2) OVER (PARTITION BY order_id ORDER BY amount_cume_dist, id) AS next_running_count_amount,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY latest_rank, id) AS row_seq
    FROM ranked_payments
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN running_sum_amount >= 6100 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_sum_amount_tier,
    order_id,
    ROUND(AVG(amount_cume_dist), 2) AS avg_amount_cume_dist,
    ROUND(AVG(row_seq), 2) AS avg_row_seq,
    COUNT(*) AS num_rows
FROM enriched
GROUP BY CASE WHEN running_sum_amount >= 6100 THEN 'PREMIUM' ELSE 'STANDARD' END, order_id
HAVING ROUND(AVG(amount_cume_dist), 2) < 1000000
ORDER BY avg_amount_cume_dist
