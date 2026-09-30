WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        ROUND(duration_sec, 1) AS duration_sec_rounded,
        CASE WHEN duration_sec >= 414 THEN 'High' WHEN duration_sec >= 188 THEN 'Medium' ELSE 'Low' END AS duration_sec_level
    FROM web_events
    WHERE event_time >= DATE '2023-07-26'
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        src.id,
        src.session_id,
        src.event_type,
        src.event_time,
        src.duration_sec,
        src.duration_sec_rounded,
        src.duration_sec_level,
        d.browser,
        d.device
    FROM filtered src
    INNER JOIN sessions d
        ON src.session_id = d.id
    WHERE d.browser IN ('Edge', 'Firefox', 'Chrome')
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(duration_sec) OVER w_recent AS last3_sum_duration_sec,
        MIN(duration_sec) OVER w_recent AS last3_min_duration_sec,
        COUNT(duration_sec) OVER w_recent AS last3_count_duration_sec
    FROM enriched
    WINDOW w_recent AS (PARTITION BY session_id, device ORDER BY duration_sec, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY device ORDER BY last3_sum_duration_sec NULLS FIRST) AS rank_in_group
        FROM metrics
    ) ranked
    WHERE rank_in_group = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, event_type
    FROM latest
    EXCEPT DISTINCT
    SELECT id, event_type
    FROM web_events
    WHERE duration_sec >= 362
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.event_type = k.event_type
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            device,
            event_type,
            browser,
            last3_min_duration_sec
        FROM reconciled
    ) src
    PIVOT (
        AVG(last3_min_duration_sec) AS amount
        FOR browser IN ('Chrome', 'Firefox', 'Edge')
    )
)
SELECT
    device,
    event_type,
    Edge,
    CASE WHEN event_type RLIKE '^(page_view|add_to_cart)$' THEN 'GROUP_B' WHEN event_type RLIKE '^(click|purchase)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS event_type_zone
FROM wide
ORDER BY device DESC NULLS LAST;
