WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), event_time) days_since_event_time,
        CONCAT(event_type, '/', page) event_type_page,
        COALESCE(page, 'UNKNOWN') page_clean
    FROM web_events
    WHERE page = '/product'
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY session_id ORDER BY days_since_event_time DESC) AS latest_rank
        FROM valid
    ) ranked
    WHERE latest_rank = 1
),
joined AS (
    -- Attach sessions attributes
    SELECT src.*, b.country
    FROM ranked src
    INNER JOIN sessions b
        ON src.session_id = b.id
    WHERE b.browser IN ('Chrome', 'Safari')
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(duration_sec) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_duration_sec,
        SUM(duration_sec) OVER w_running cumulative_duration_sec_2,
        FIRST_VALUE(page) IGNORE NULLS OVER (PARTITION BY event_type ORDER BY days_since_event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_page,
        SUM(days_since_event_time) OVER w_part event_time_period_sum_days_since_event_time
    FROM joined
    WINDOW w_running AS (PARTITION BY country ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY DATE_TRUNC('week', event_time), event_type)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM scored
    QUALIFY RANK() OVER (PARTITION BY event_type, session_id ORDER BY latest_rank) = 1
)
SELECT
    id,
    event_type,
    page,
    event_time,
    duration_sec,
    days_since_event_time,
    event_type_page,
    latest_rank,
    country
FROM deduped
