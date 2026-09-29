WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM sessions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY browser, country ORDER BY started_at DESC NULLS LAST, id) = 1
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.duration_sec
    FROM latest s
    LEFT JOIN LATERAL (
            SELECT c.duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), started_at) / 365.25 AS years_since_started_at,
        COALESCE(country, 'UNKNOWN') AS country_filled
    FROM with_last
    WHERE country IS NOT NULL
      AND device = 'desktop'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY device ORDER BY duration_sec) AS duration_sec_percent_rank,
        MAX(years_since_started_at) OVER (PARTITION BY country ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS years_since_started_at_max_30d
    FROM cleaned
    WHERE HOUR(started_at) BETWEEN 11 AND 19
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.session_id = 16
        )
),
cleaned_sessions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(years_since_started_at * 1.15, 2) AS years_since_started_at_adjusted,
        COALESCE(country, 'DIRECT') AS country_filled_2
    FROM kept
    WHERE started_at > TIMESTAMP '2023-05-29 08:00:00'
      AND customer_id IS NOT NULL
)
SELECT
    id,
    country,
    years_since_started_at,
    country_filled,
    years_since_started_at_max_30d,
    years_since_started_at_adjusted,
    country_filled_2,
    CASE WHEN country RLIKE '^(UK|SG)$' THEN 'GROUP_A' WHEN country RLIKE '^(TH|US)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS country_zone
FROM cleaned_sessions
ORDER BY years_since_started_at;
