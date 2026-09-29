WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        device,
        started_at,
        country,
        INITCAP(TRIM(device)) AS device_title,
        TO_DATE(started_at) AS started_at_date
    FROM sessions
    WHERE started_at > DATE '2023-08-24'
      AND country IS NOT NULL
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at, id) AS rank_in_group
    FROM valid
    QUALIFY rank_in_group = 1
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM latest
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', started_at) AS quarter_start,
    CASE WHEN session_id >= 7 THEN 'LARGE' ELSE 'SMALL' END AS session_id_tier,
    APPROX_PERCENTILE(session_id, 0.9) AS session_id_p90,
    COUNT_IF(country = 'SG') AS sg_events,
    COUNT_IF(device = 'tablet') AS tablet_events
FROM islands
GROUP BY CUBE (DATE_TRUNC('QUARTER', started_at), CASE WHEN session_id >= 7 THEN 'LARGE' ELSE 'SMALL' END)
