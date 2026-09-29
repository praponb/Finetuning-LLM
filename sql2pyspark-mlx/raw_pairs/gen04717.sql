WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY monthly_fee NULLS FIRST) monthly_fee_cume_dist,
        SUM(monthly_fee) OVER (PARTITION BY customer_id ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_monthly_fee,
        LAG(monthly_fee, 1) OVER (PARTITION BY plan ORDER BY monthly_fee, id) prev_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date, id) event_seq
    FROM subscriptions
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        plan,
        start_date,
        cumulative_monthly_fee,
        event_seq
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY monthly_fee, id) AS rank_in_group
        FROM with_windows
    ) ranked
    WHERE rank_in_group = 1
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, plan
    FROM latest
    INTERSECT
    SELECT id, plan
    FROM subscriptions
    WHERE customer_id IS NOT NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    JOIN kept_keys k ON s.id = k.id AND s.plan = k.plan
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        INITCAP(TRIM(plan)) plan_title,
        CASE WHEN plan = 'premium' THEN 1 ELSE 0 END is_premium
    FROM reconciled
    WHERE start_date >= '2024-05-19'
)
-- Grouped metrics
SELECT
    CASE WHEN event_seq >= 9 THEN 'HIGH' ELSE 'LOW' END AS event_seq_tier,
    plan,
    DATE_TRUNC('QUARTER', start_date) AS quarter_start,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    PERCENTILE_APPROX(is_premium, 0.5) AS is_premium_p50,
    SUM(event_seq) AS total_event_seq,
    AVG(is_premium) AS avg_is_premium
FROM filtered
GROUP BY ROLLUP (CASE WHEN event_seq >= 9 THEN 'HIGH' ELSE 'LOW' END, plan, DATE_TRUNC('QUARTER', start_date))
HAVING PERCENTILE_APPROX(is_premium, 0.5) >= 0
ORDER BY total_event_seq DESC
