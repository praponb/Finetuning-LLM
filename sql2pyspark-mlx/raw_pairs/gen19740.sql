WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(duration_sec) OVER (PARTITION BY TO_DATE(event_time) ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec,
        NTILE(4) OVER (PARTITION BY event_type ORDER BY duration_sec, id) AS duration_sec_bucket,
        SUM(duration_sec) OVER w_recent AS duration_sec_sum_90d
    FROM web_events
    WINDOW w_recent AS (PARTITION BY YEAR(event_time) ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        session_id,
        page,
        duration_sec,
        cumulative_duration_sec,
        duration_sec_bucket,
        duration_sec_sum_90d,
        cumulative_duration_sec * duration_sec_bucket AS cumulative_duration_sec_times_duration_sec_bucket
    FROM enriched
    WHERE page IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY session_id, page ORDER BY duration_sec_bucket DESC, id) = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        session_id,
        page,
        duration_sec,
        duration_sec_bucket,
        duration_sec_sum_90d,
        cumulative_duration_sec_times_duration_sec_bucket,
        LEAD(cumulative_duration_sec) OVER w_hist AS next_cumulative_duration_sec,
        LAG(cumulative_duration_sec) OVER w_hist AS prev_cumulative_duration_sec,
        LAG(duration_sec_bucket) OVER w_hist AS prev_duration_sec_bucket,
        LEAD(duration_sec_bucket) OVER w_hist AS next_duration_sec_bucket,
        ROW_NUMBER() OVER w_hist AS row_seq,
        SUM(duration_sec_sum_90d) OVER (PARTITION BY session_id ORDER BY duration_sec_sum_90d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec_sum_90d
    FROM ranked
    WHERE cumulative_duration_sec_times_duration_sec_bucket > 39
    WINDOW w_hist AS (PARTITION BY session_id, page ORDER BY cumulative_duration_sec, id)
),
joined AS (
    -- Attach sessions attributes
    SELECT
        s.*,
        p2.device,
        p2.country
    FROM scored s
    LEFT JOIN sessions p2
        ON s.session_id = p2.id
),
joined_web_events AS (
    -- Attach sessions attributes
    SELECT
        e.*,
        p.device AS session_device,
        p.country AS session_country,
        p.browser
    FROM joined e
    LEFT JOIN sessions p
        ON e.session_id = p.id
),
joined_web_events_2 AS (
    -- Attach sessions attributes
    SELECT
        e.id,
        e.session_id,
        e.duration_sec_sum_90d,
        e.cumulative_duration_sec_times_duration_sec_bucket,
        e.prev_cumulative_duration_sec,
        e.prev_duration_sec_bucket,
        e.row_seq,
        e.cumulative_duration_sec_sum_90d,
        e.device,
        e.session_country,
        d.browser AS session_browser,
        d.country AS session_country_2,
        d.device AS session_device_2
    FROM joined_web_events e
    LEFT JOIN sessions d
        ON e.session_id = d.id
)
-- Join a dimension and aggregate
SELECT
    s.country,
    e.session_device_2,
    COUNT(*) AS num_rows,
    MAX(e.cumulative_duration_sec_sum_90d) AS max_cumulative_duration_sec_sum_90d
FROM joined_web_events_2 e
INNER JOIN sessions s ON e.session_id = s.id
GROUP BY ROLLUP (s.country, e.session_device_2)
ORDER BY max_cumulative_duration_sec_sum_90d NULLS LAST;
