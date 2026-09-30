WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_TRUNC('week', event_time) AS event_time_period,
        LOWER(event_type) AS event_type_lower,
        COALESCE(page, 'DIRECT') AS resolved_page
    FROM web_events
    WHERE event_type <> 'page_view'
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        s.id,
        s.session_id,
        s.event_type,
        s.page,
        s.duration_sec,
        d.device,
        d.country,
        d.browser
    FROM filtered s
    LEFT JOIN sessions d
        ON s.session_id = d.id
    WHERE d.customer_id IS NOT NULL
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(duration_sec) OVER (PARTITION BY device) AS device_sum_duration_sec,
        LAG(duration_sec, 1) OVER (ORDER BY duration_sec, id) AS prev_duration_sec
    FROM enriched
),
scored_web_events AS (
    -- Running and moving aggregates
    SELECT
        id,
        duration_sec,
        country,
        prev_duration_sec,
        COUNT(device_sum_duration_sec) OVER (PARTITION BY device, country) AS device_count_device_sum_duration_sec,
        FIRST_VALUE(country) IGNORE NULLS OVER (PARTITION BY page ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country,
        PERCENT_RANK() OVER (ORDER BY device_sum_duration_sec NULLS LAST) AS device_sum_duration_sec_percent_rank
    FROM scored
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(prev_duration_sec, 2) AS prev_duration_sec_rounded,
        CASE WHEN duration_sec >= 397 THEN 'High' WHEN duration_sec >= 189 THEN 'Medium' ELSE 'Low' END AS duration_sec_tier,
        COALESCE(country, 'DIRECT') AS country_filled
    FROM scored_web_events
    WHERE country IS NOT NULL
      AND country IS NOT DISTINCT FROM 'UK'
)
-- Multi-level aggregation
SELECT
    CASE WHEN device_sum_duration_sec_percent_rank >= 0.83 THEN 'LARGE' ELSE 'SMALL' END AS device_sum_duration_sec_percent_rank_tier,
    CASE WHEN device_count_device_sum_duration_sec >= 16 THEN 'HIGH' ELSE 'LOW' END AS device_count_device_sum_duration_sec_tier,
    COUNT(CASE WHEN country = 'US' THEN 1 END) AS us_count,
    MIN(prev_duration_sec) AS min_prev_duration_sec,
    AVG(device_sum_duration_sec_percent_rank) FILTER (WHERE country = 'TH') AS th_avg_device_sum_duration_sec_percent_rank,
    COUNT(DISTINCT id) AS distinct_ids,
    BIT_AND(duration_sec) AS duration_sec_bit_and,
    GROUPING(CASE WHEN device_sum_duration_sec_percent_rank >= 0.83 THEN 'LARGE' ELSE 'SMALL' END) AS gid
FROM base
GROUP BY ROLLUP (CASE WHEN device_sum_duration_sec_percent_rank >= 0.83 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN device_count_device_sum_duration_sec >= 16 THEN 'HIGH' ELSE 'LOW' END)
HAVING AVG(device_sum_duration_sec_percent_rank) FILTER (WHERE country = 'TH') > 0
ORDER BY th_avg_device_sum_duration_sec_percent_rank;
