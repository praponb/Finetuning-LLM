WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(device, 1, 2) AS device_prefix,
        COALESCE(country, 'DIRECT') AS resolved_country
    FROM sessions
    WHERE started_at >= TIMESTAMP '2023-07-23 12:00:00'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time < s.started_at
        )
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.page <> '/product'
        )
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM not_flagged
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        island_id,
        COUNT(new_group_flag) OVER (PARTITION BY YEAR(started_at), browser) AS started_at_year_count_new_group_flag,
        SUM(island_id) OVER (PARTITION BY DATE_TRUNC('MONTH', started_at), device ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS island_id_sum_30d,
        SUM(island_id) OVER (PARTITION BY browser ORDER BY new_group_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_island_id,
        SUM(new_group_flag) OVER (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_new_group_flag,
        AVG(island_id) OVER (ORDER BY new_group_flag, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS island_id_avg_2
    FROM sessions_sessions
    WHERE customer_id IN (10, 19, 21, 28)
)
SELECT
    s.id,
    s.customer_id,
    s.island_id,
    s.started_at_year_count_new_group_flag,
    s.cumulative_new_group_flag,
    s.island_id_avg_2,
    (SELECT AVG(x.balance) FROM accounts x WHERE x.customer_id = s.customer_id) AS avg_account_balance
FROM scored s
ORDER BY island_id_avg_2 NULLS FIRST, cumulative_new_group_flag
