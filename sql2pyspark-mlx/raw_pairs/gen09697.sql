WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT_WS('-', event_type, page) AS event_type_page_key,
        LENGTH(event_type) AS event_type_length,
        COALESCE(page, 'NONE') AS page_clean
    FROM web_events
    WHERE HOUR(event_time) BETWEEN 11 AND 15
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        page,
        event_time,
        event_type_page_key,
        page_clean,
        CAST(duration_sec AS DOUBLE) AS duration_sec_num,
        ROUND(event_type_length * 1.15, 2) AS event_type_length_scaled
    FROM base
    WHERE event_time > DATE '2023-07-09'
      AND session_id IN (10, 40)
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, session_id
    FROM cleaned
    EXCEPT DISTINCT
    SELECT id, session_id
    FROM web_events
    WHERE TO_DATE(event_time) < '2024-08-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.session_id = k.session_id
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        a.session_id,
        a.event_type_page_key,
        s.device
    FROM kept_rows a
    LEFT JOIN sessions s
        ON a.session_id = s.id
    WHERE s.started_at >= '2024-10-21'
),
matched_keys AS (
    -- Set difference on keys
    SELECT session_id
    FROM with_dims
    EXCEPT
    SELECT session_id
    FROM web_events
    WHERE page IN ('/checkout', '/product', '/cart')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN matched_keys k ON s.session_id = k.session_id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(device) AS device_lower,
        COALESCE(device, 'N/A') AS device_clean
    FROM reconciled
    WHERE device IS NOT NULL
),
base_web_events AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        session_id,
        event_type_page_key,
        device,
        device_lower,
        device_clean,
        CASE WHEN device = 'tablet' THEN 1 ELSE 0 END AS is_tablet,
        COALESCE(device, 'OTHER') AS device_clean_2
    FROM filtered
    WHERE device IS NOT NULL
)
SELECT
    s.session_id,
    s.device_clean,
    s.device_clean_2,
    (SELECT MAX(t2.duration_sec) FROM web_events t2 WHERE t2.session_id = s.session_id) AS max_web_event_duration_sec
FROM base_web_events s
ORDER BY session_id ASC NULLS LAST;
