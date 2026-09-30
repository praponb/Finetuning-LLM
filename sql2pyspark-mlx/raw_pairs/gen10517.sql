WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(10) OVER w_hist AS duration_sec_bucket,
        PERCENT_RANK() OVER (PARTITION BY session_id, event_type ORDER BY duration_sec DESC NULLS LAST) AS duration_sec_percent_rank,
        FIRST_VALUE(duration_sec) IGNORE NULLS OVER (PARTITION BY page ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec
    FROM web_events
    WINDOW w_hist AS (PARTITION BY DATE_TRUNC('month', event_time), event_type ORDER BY duration_sec DESC, id)
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        src.*,
        b.country,
        b.device,
        b.browser
    FROM with_windows src
    LEFT JOIN sessions b
        ON src.session_id = b.id
),
joined AS (
    -- Attach sessions attributes
    SELECT
        src.*,
        d.device AS session_device,
        d.browser AS session_browser
    FROM with_dims src
    JOIN sessions d
        ON src.session_id = d.id
    WHERE d.country = 'UK'
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT event_type, page
    FROM joined
    INTERSECT
    SELECT event_type, page
    FROM web_events
    WHERE session_id = 19
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    JOIN remaining_keys k ON s.event_type = k.event_type AND s.page = k.page
)
SELECT
    s.id,
    s.session_id,
    s.event_type,
    s.page,
    s.event_time,
    s.duration_sec,
    s.duration_sec_bucket,
    s.device,
    s.browser,
    CASE WHEN browser RLIKE '^(Firefox|Chrome)$' THEN 'CORE' ELSE 'GROUP_A' END AS browser_zone,
    (SELECT AVG(w2.duration_sec) FROM web_events w2 WHERE w2.session_id = s.session_id) AS avg_web_event_duration_sec
FROM verified s;
