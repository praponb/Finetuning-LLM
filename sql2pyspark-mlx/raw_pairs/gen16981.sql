WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(duration_sec) OVER (PARTITION BY page, event_type ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_duration_sec,
        AVG(duration_sec) OVER (PARTITION BY page, event_type ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_duration_sec,
        MAX(duration_sec) OVER (PARTITION BY page, event_type ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_duration_sec
    FROM web_events
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, duration_sec - running_avg_duration_sec AS duration_sec_minus_running_avg_duration_sec
    FROM scored
    WHERE event_time >= DATE '2023-04-09'
      AND event_type NOT IN ('search', 'click', 'purchase')
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        session_id,
        event_type,
        event_time,
        running_sum_duration_sec,
        running_avg_duration_sec
    FROM valid
    QUALIFY RANK() OVER (PARTITION BY page, session_id ORDER BY event_time NULLS FIRST) = 1
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        event_type,
        event_time,
        running_sum_duration_sec,
        running_avg_duration_sec,
        ROUND(running_avg_duration_sec, 0) AS running_avg_duration_sec_rounded,
        DATE_FORMAT(event_time, 'yyyy-MM') AS event_time_label
    FROM latest
    WHERE event_time >= DATE '2023-10-14'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY event_type ORDER BY running_avg_duration_sec DESC, id) AS running_avg_duration_sec_bucket,
        SUM(running_avg_duration_sec_rounded) OVER (PARTITION BY session_id ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS running_avg_duration_sec_rounded_sum_90d,
        FIRST_VALUE(running_avg_duration_sec, TRUE) OVER (PARTITION BY session_id, event_type ORDER BY running_avg_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_avg_duration_sec,
        CUME_DIST() OVER (ORDER BY running_sum_duration_sec DESC NULLS LAST) AS running_sum_duration_sec_cume_dist,
        SUM(running_avg_duration_sec_rounded) OVER (PARTITION BY session_id, event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_avg_duration_sec_rounded
    FROM filtered
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN first_known_running_avg_duration_sec >= 470 THEN 'HIGH' ELSE 'LOW' END AS first_known_running_avg_duration_sec_tier,
        ROUND(SUM(running_avg_duration_sec_rounded_sum_90d) / NULLIF(COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END), 0), 2) AS running_avg_duration_sec_rounded_sum_90d_per_add_to_cart,
        COUNT_IF(event_type = 'purchase') AS purchase_events,
        COUNT(*) FILTER (WHERE event_type = 'page_view') AS page_view_rows,
        ROUND(SUM(cumulative_running_avg_duration_sec_rounded) / NULLIF(COUNT(CASE WHEN event_type = 'purchase' THEN 1 END), 0), 2) AS cumulative_running_avg_duration_sec_rounded_per_purchase
    FROM enriched
    GROUP BY CASE WHEN first_known_running_avg_duration_sec >= 470 THEN 'HIGH' ELSE 'LOW' END
    HAVING COUNT(*) FILTER (WHERE event_type = 'page_view') < 1000
)
SELECT
    first_known_running_avg_duration_sec_tier,
    running_avg_duration_sec_rounded_sum_90d_per_add_to_cart,
    purchase_events,
    page_view_rows,
    cumulative_running_avg_duration_sec_rounded_per_purchase
FROM daily
ORDER BY cumulative_running_avg_duration_sec_rounded_per_purchase DESC NULLS FIRST;
