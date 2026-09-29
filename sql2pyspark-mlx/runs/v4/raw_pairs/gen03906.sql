WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT_WS('-', device, country) AS device_country_key
    FROM sessions
    WHERE started_at > TO_DATE('2024-01-03')
      AND country IS NOT NULL
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM cleaned
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.web_event_event_time
    FROM sessionized s
    JOIN LATERAL (
            SELECT c.event_time AS web_event_event_time
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time >= s.started_at
        )
),
with_dims AS (
    -- Attach customers attributes
    SELECT src.*, p.age
    FROM kept src
    LEFT JOIN customers p
        ON src.customer_id = p.id
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM with_dims
    EXCEPT DISTINCT
    SELECT id
    FROM sessions
    WHERE device != 'tablet'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN remaining_keys k ON s.id = k.id
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM verified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.session_id = 33
        )
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN age >= 66 THEN 'HIGH' ELSE 'LOW' END AS age_tier,
    CASE WHEN island_id >= 8 THEN 'HIGH' ELSE 'LOW' END AS island_id_tier,
    SIZE(COLLECT_SET(country)) AS num_countries,
    PERCENTILE_APPROX(new_group_flag, ARRAY(0.25, 0.5, 0.75)) AS new_group_flag_quartiles,
    MAX(web_event_event_time) AS ended_at,
    GROUPING_ID() AS grouping_id
FROM not_flagged
GROUP BY ROLLUP (CASE WHEN age >= 66 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN island_id >= 8 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY island_id_tier
