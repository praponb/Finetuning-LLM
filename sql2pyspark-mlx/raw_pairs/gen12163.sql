WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, LENGTH(device) AS device_length
    FROM sessions
    WHERE country IS NOT NULL
),
latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.web_event_event_time,
        latest.web_event_event_type
    FROM base s
    JOIN LATERAL (
            SELECT c.event_time AS web_event_event_time, c.event_type AS web_event_event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        customer_id,
        started_at,
        country
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY device_length ASC NULLS FIRST, id ASC) AS rn
        FROM latest_match
    ) x
    WHERE rn = 1
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time >= s.started_at
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
              AND ch.event_time >= s.started_at
        )
),
totals AS (
    -- Grouped metrics
    SELECT
        country,
        DATE_TRUNC('WEEK', started_at) AS week_start,
        COUNT(DISTINCT customer_id) AS distinct_customer_ids,
        COUNT(CASE WHEN country = 'US' THEN 1 END) AS us_count,
        SORT_ARRAY(COLLECT_SET(country)) AS country_list,
        COUNT(DISTINCT id) AS distinct_ids
    FROM not_flagged
    GROUP BY country, DATE_TRUNC('WEEK', started_at)
)
SELECT
    country,
    week_start,
    us_count,
    distinct_ids,
    CASE WHEN country RLIKE '^(UK|TH)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS country_zone
FROM totals
ORDER BY country_zone DESC NULLS LAST
