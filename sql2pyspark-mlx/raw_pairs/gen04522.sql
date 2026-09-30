WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        session_id,
        event_type,
        page,
        DATE_FORMAT(event_time, 'yyyy-MM-dd') AS event_time_label,
        ROUND(duration_sec, 0) AS duration_sec_rounded
    FROM web_events
    WHERE page IS NOT NULL
      AND event_type IN ('page_view', 'click')
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        session_id,
        DENSE_RANK() OVER (PARTITION BY session_id ORDER BY duration_sec_rounded DESC) AS duration_sec_rounded_dense_rank,
        RANK() OVER (PARTITION BY session_id, event_type ORDER BY duration_sec_rounded NULLS FIRST) AS duration_sec_rounded_rank,
        COUNT(duration_sec_rounded) OVER (PARTITION BY event_type, session_id) AS event_type_count_duration_sec_rounded
    FROM filtered
    WHERE page = '/product'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec_rounded_rank DESC NULLS FIRST, id) = 1
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        a.id,
        a.session_id,
        a.duration_sec_rounded_dense_rank,
        a.event_type_count_duration_sec_rounded,
        d.device,
        d.country,
        d.browser
    FROM ranked a
    INNER JOIN sessions d
        ON a.session_id = d.id
),
matched_keys AS (
    -- Set difference on keys
    SELECT session_id
    FROM with_dims
    EXCEPT
    SELECT session_id
    FROM web_events
    WHERE duration_sec BETWEEN 229 AND 230
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN matched_keys k ON s.session_id = k.session_id
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN duration_sec_rounded_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS duration_sec_rounded_dense_rank_tier,
        AVG(event_type_count_duration_sec_rounded) FILTER (WHERE country = 'TH') AS th_avg_event_type_count_duration_sec_rounded,
        ROUND(AVG(duration_sec_rounded_dense_rank), 2) AS avg_duration_sec_rounded_dense_rank,
        SUM(event_type_count_duration_sec_rounded) FILTER (WHERE device = 'mobile') AS mobile_total_event_type_count_duration_sec_rounded,
        PERCENTILE_APPROX(duration_sec_rounded_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS duration_sec_rounded_dense_rank_quartiles
    FROM kept_rows
    GROUP BY CASE WHEN duration_sec_rounded_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END
    HAVING avg_duration_sec_rounded_dense_rank >= 0
)
SELECT
    duration_sec_rounded_dense_rank_tier,
    th_avg_event_type_count_duration_sec_rounded,
    mobile_total_event_type_count_duration_sec_rounded,
    duration_sec_rounded_dense_rank_quartiles
FROM totals;
