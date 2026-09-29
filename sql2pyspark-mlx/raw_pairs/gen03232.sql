WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        SUBSTRING(method, 1, 4) method_prefix,
        COALESCE(method, 'N/A') method_clean
    FROM payments
    WHERE paid_at >= DATE '2023-09-18'
      AND order_id IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(amount) OVER w_grp method_sum_amount,
        AVG(amount) OVER (PARTITION BY method ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) amount_avg_6
    FROM filtered
    WHERE method = 'cash'
    WINDOW w_grp AS (PARTITION BY method)
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM enriched
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        method,
        method_prefix,
        burst_id
    FROM islands
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount_avg_6 DESC NULLS LAST, id) = 1
)
SELECT
    id,
    method,
    method_prefix
FROM ranked
