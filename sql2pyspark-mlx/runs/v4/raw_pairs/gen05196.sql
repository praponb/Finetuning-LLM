WITH enriched AS (
    -- Attach sessions attributes
    SELECT
        a.*,
        p.country,
        p.device,
        p.browser
    FROM web_events a
    LEFT JOIN sessions p
        ON a.session_id = p.id
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN browser IN ('Safari', 'Edge') THEN 'Tier 1' ELSE 'Other' END AS browser_group
    FROM enriched
    WHERE event_time > TIMESTAMP '2023-11-28 14:00:00'
      AND browser IS NOT NULL
      AND browser = 'Safari'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY event_time NULLS FIRST, id) AS latest_rank
        FROM valid
    ) ranked
    WHERE latest_rank = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY duration_sec, id) AS event_seq,
        AVG(duration_sec) OVER (ORDER BY duration_sec, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS duration_sec_avg_6_prior,
        SUM(duration_sec) OVER (PARTITION BY page, device ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS duration_sec_sum_7d,
        AVG(duration_sec) OVER (ORDER BY duration_sec, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS duration_sec_avg_5_prior
    FROM deduped
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY country ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY country ORDER BY event_time, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM with_windows
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY country ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
SELECT
    id,
    event_type,
    page,
    event_time,
    duration_sec,
    device,
    browser_group,
    latest_rank,
    duration_sec_avg_6_prior
FROM sessions_web_events
ORDER BY duration_sec;
