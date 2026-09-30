WITH enriched AS (
    -- Attach sessions attributes
    SELECT
        s.*,
        d.browser,
        d.country
    FROM web_events s
    LEFT JOIN sessions d
        ON s.session_id = d.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY YEAR(event_time), event_type ORDER BY duration_sec NULLS LAST) AS duration_sec_rank,
        MAX(duration_sec) OVER (PARTITION BY session_id) AS session_id_max_duration_sec
    FROM enriched
    WHERE event_type = 'purchase'
),
enriched_web_events AS (
    -- Ranking and gap features
    SELECT
        id,
        event_type,
        page,
        duration_sec,
        country,
        duration_sec_rank,
        COUNT(duration_sec) OVER w_hist AS running_count_duration_sec,
        MIN(duration_sec) OVER w_hist AS running_min_duration_sec,
        MAX(duration_sec) OVER w_hist AS running_max_duration_sec
    FROM metrics
    WINDOW w_hist AS (PARTITION BY country ORDER BY session_id_max_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
matched_keys AS (
    -- Set difference on keys
    SELECT event_type, page
    FROM enriched_web_events
    EXCEPT DISTINCT
    SELECT event_type, page
    FROM web_events
    WHERE duration_sec >= 275
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched_web_events s
    INNER JOIN matched_keys k ON s.event_type = k.event_type AND s.page = k.page
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        event_type,
        page,
        country,
        duration_sec_rank,
        AVG(duration_sec) OVER (PARTITION BY country) AS country_avg_duration_sec
    FROM kept_rows
)
-- Grouped metrics
SELECT
    CASE WHEN country_avg_duration_sec >= 454 THEN 'HIGH' ELSE 'LOW' END AS country_avg_duration_sec_tier,
    COUNT(DISTINCT country) AS distinct_countries,
    PERCENTILE_APPROX(country_avg_duration_sec, 0.9) AS country_avg_duration_sec_p90,
    SUM(country_avg_duration_sec) AS total_country_avg_duration_sec
FROM scored
GROUP BY CASE WHEN country_avg_duration_sec >= 454 THEN 'HIGH' ELSE 'LOW' END
ORDER BY total_country_avg_duration_sec NULLS LAST;
