WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        LOWER(page) page_lower,
        TRIM(event_type) clean_event_type,
        COALESCE(page, 'N/A') page_clean
    FROM web_events
    WHERE event_time > TIMESTAMP '2023-12-25 17:00:00'
      AND page IS NULL
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM base
    QUALIFY RANK() OVER (PARTITION BY event_type, session_id ORDER BY duration_sec) = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(duration_sec) OVER (ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) duration_sec_sum_90d,
        SUM(duration_sec) OVER w_part cumulative_duration_sec,
        LAST_VALUE(duration_sec) IGNORE NULLS OVER w_hist last_known_duration_sec
    FROM ranked
    WINDOW w_part AS (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        e.*,
        b.country,
        b.device
    FROM scored e
    INNER JOIN sessions b
        ON e.session_id = b.id
),
matched_keys AS (
    -- Set intersection on keys
    SELECT session_id
    FROM with_dims
    INTERSECT ALL
    SELECT session_id
    FROM web_events
    WHERE duration_sec <= 271
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN matched_keys k ON s.session_id = k.session_id
),
stats AS (
    -- Grouped metrics
    SELECT
        country,
        CASE WHEN duration_sec_sum_90d >= 2634 THEN 'PREMIUM' ELSE 'STANDARD' END AS duration_sec_sum_90d_tier,
        CASE WHEN cumulative_duration_sec >= 2677 THEN 'HIGH' ELSE 'LOW' END AS cumulative_duration_sec_tier,
        ROUND(SUM(last_known_duration_sec) / NULLIF(COUNT(CASE WHEN page = '/cart' THEN 1 END), 0), 2) AS last_known_duration_sec_per_cart,
        MIN(event_time) AS started_at
    FROM kept_rows
    GROUP BY ROLLUP (country, CASE WHEN duration_sec_sum_90d >= 2634 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN cumulative_duration_sec >= 2677 THEN 'HIGH' ELSE 'LOW' END)
)
SELECT
    country,
    duration_sec_sum_90d_tier,
    cumulative_duration_sec_tier,
    last_known_duration_sec_per_cart,
    CASE WHEN country RLIKE '^(US|DE)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS country_zone
FROM stats
ORDER BY country_zone DESC NULLS FIRST;
