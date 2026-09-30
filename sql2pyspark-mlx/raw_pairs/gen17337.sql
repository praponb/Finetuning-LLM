WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM sessions s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.duration_sec BETWEEN 296 AND 474
        )
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM eligible
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM sessionized src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.started_at >= sub.start_date
       AND src.started_at < sub.end_date + INTERVAL '30' DAY
),
flagged_2 AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id, subscription_id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM matched
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device,
        browser,
        started_at,
        country,
        new_group_flag,
        subscription_id,
        SUM(is_new_burst) OVER (PARTITION BY browser ORDER BY started_at, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged_2
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(subscription_id, TRUE) OVER (PARTITION BY device ORDER BY subscription_id, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_subscription_id,
        AVG(subscription_id) OVER w_hist AS subscription_id_avg_3_prior
    FROM islands
    WHERE island_id > 4
    WINDOW w_hist AS (PARTITION BY device ORDER BY island_id, id, subscription_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        device,
        country,
        new_group_flag,
        subscription_id,
        island_id
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY browser ORDER BY subscription_id, id, subscription_id) = 1
)
-- Grouped metrics
SELECT
    CASE WHEN new_group_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS new_group_flag_tier,
    COUNT(*) FILTER (WHERE country = 'DE') AS de_rows,
    APPROX_PERCENTILE(new_group_flag, ARRAY(0.25, 0.5, 0.75)) AS new_group_flag_quartiles
FROM deduped
GROUP BY ROLLUP (CASE WHEN new_group_flag >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY new_group_flag_tier DESC;
