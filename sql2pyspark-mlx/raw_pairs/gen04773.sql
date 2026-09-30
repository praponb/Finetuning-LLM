WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(duration_sec AS STRING) AS duration_sec_str,
        CASE WHEN page = '/search' THEN 1 ELSE 0 END AS is_search,
        COALESCE(page, 'UNKNOWN') AS resolved_page
    FROM web_events
    WHERE page IS NOT NULL
      AND page IS NOT DISTINCT FROM '/home'
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        src.id,
        src.session_id,
        src.event_type,
        src.event_time,
        src.duration_sec,
        src.duration_sec_str,
        src.is_search,
        s.device,
        s.country,
        s.browser
    FROM base src
    LEFT JOIN sessions s
        ON src.session_id = s.id
    WHERE s.country IN ('US', 'UK')
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(duration_sec) OVER (PARTITION BY session_id ORDER BY is_search, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS duration_sec_avg_5,
        RANK() OVER (PARTITION BY country ORDER BY duration_sec) AS duration_sec_rank,
        FIRST_VALUE(device) IGNORE NULLS OVER (PARTITION BY country ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_device,
        AVG(is_search) OVER (PARTITION BY device, browser ORDER BY is_search, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS is_search_avg_2_prior
    FROM enriched
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device, browser ORDER BY is_search NULLS FIRST, id) AS row_num
    FROM scored
    QUALIFY row_num = 1
)
-- Grouped metrics
SELECT
    CASE WHEN duration_sec_avg_5 >= 521 THEN 'HIGH' ELSE 'LOW' END AS duration_sec_avg_5_tier,
    PERCENTILE_APPROX(is_search, ARRAY(0.25, 0.5, 0.75)) AS is_search_quartiles,
    APPROX_PERCENTILE(duration_sec, 0.95) AS duration_sec_p95,
    BIT_OR(duration_sec) AS duration_sec_bit_or,
    COUNT(CASE WHEN device = 'mobile' THEN 1 END) AS mobile_count,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS duration_min
FROM deduped
GROUP BY ROLLUP (CASE WHEN duration_sec_avg_5 >= 521 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY duration_sec_p95 NULLS LAST
