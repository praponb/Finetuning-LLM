WITH with_dims AS (
    -- Attach sessions attributes
    SELECT
        e.id,
        e.session_id,
        e.event_type,
        e.page,
        e.event_time,
        e.duration_sec,
        d.country,
        d.device
    FROM web_events e
    LEFT JOIN sessions d
        ON e.session_id = d.id
    WHERE d.browser = 'Firefox'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_dims
    QUALIFY RANK() OVER (PARTITION BY page, country ORDER BY duration_sec) = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('WEEK', event_time), country ORDER BY duration_sec, id) row_seq,
        SUM(duration_sec) OVER (PARTITION BY country, event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_duration_sec
    FROM deduped
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        event_time,
        duration_sec,
        country,
        row_seq,
        cumulative_duration_sec,
        DATE_TRUNC('month', event_time) event_time_period,
        ROUND(duration_sec / row_seq, 2) duration_sec_per_row_seq,
        COALESCE(country, 'N/A') country_filled
    FROM scored
    WHERE country IS NOT NULL
      AND row_seq BETWEEN 5 AND 6
)
-- Grouped metrics
SELECT
    CASE WHEN duration_sec >= 483 THEN 'PREMIUM' ELSE 'STANDARD' END AS duration_sec_tier,
    country,
    COUNT(*) AS num_rows,
    APPROX_PERCENTILE(duration_sec_per_row_seq, 0.95) AS duration_sec_per_row_seq_p95,
    AVG(cumulative_duration_sec) FILTER (WHERE country = 'SG') AS sg_avg_cumulative_duration_sec,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list
FROM valid
GROUP BY CASE WHEN duration_sec >= 483 THEN 'PREMIUM' ELSE 'STANDARD' END, country;
