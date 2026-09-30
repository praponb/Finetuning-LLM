WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN page = '/cart' THEN 1 ELSE 0 END AS is_cart,
        CONCAT(page, ' - ', event_type) AS page_event_type
    FROM web_events
    WHERE page IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(is_cart) OVER (PARTITION BY session_id ORDER BY duration_sec, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS is_cart_avg_2,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time DESC, id) AS row_seq
    FROM filtered
),
enriched AS (
    -- Attach sessions attributes
    SELECT s.*, b.country
    FROM with_windows s
    LEFT JOIN sessions b
        ON s.session_id = b.id
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN duration_sec >= 462 THEN 'High' WHEN duration_sec >= 206 THEN 'Medium' ELSE 'Low' END AS duration_sec_band,
        INITCAP(TRIM(event_type)) AS event_type_title
    FROM enriched
    WHERE duration_sec = 405
),
enriched_web_events AS (
    -- Ranking and gap features
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        is_cart,
        page_event_type,
        is_cart_avg_2,
        row_seq,
        country,
        duration_sec_band,
        event_type_title,
        FIRST_VALUE(duration_sec, TRUE) OVER (PARTITION BY page ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec,
        AVG(duration_sec) OVER (PARTITION BY page ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS duration_sec_avg_7d
    FROM valid
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        src.*,
        b.browser,
        b.device,
        b.country AS session_country
    FROM enriched_web_events src
    JOIN sessions b
        ON src.session_id = b.id
),
joined AS (
    -- Attach sessions attributes
    SELECT
        src.*,
        d.device AS session_device,
        d.country AS session_country_2
    FROM with_dims src
    LEFT JOIN sessions d
        ON src.session_id = d.id
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', event_time) AS week_start,
    DATE_TRUNC('MONTH', event_time) AS activity_month,
    BIT_OR(duration_sec) AS duration_sec_bit_or,
    COUNT(CASE WHEN session_country = 'DE' THEN 1 END) AS de_count,
    BIT_AND(duration_sec) AS duration_sec_bit_and
FROM joined
GROUP BY DATE_TRUNC('WEEK', event_time), DATE_TRUNC('MONTH', event_time);
