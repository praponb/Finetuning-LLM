WITH valid AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        DATE_FORMAT(event_time, 'MMM yyyy') AS event_time_label
    FROM web_events
    WHERE page IS DISTINCT FROM '/cart'
      AND event_time >= '2024-10-01'
),
scored AS (
    SELECT
        id,
        page,
        event_time,
        event_time_label,
        LEAD(duration_sec) OVER (PARTITION BY event_type, page ORDER BY event_time, id) AS next_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY event_type, page ORDER BY event_time, id) AS row_seq,
        MAX(duration_sec) OVER (PARTITION BY event_type ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS duration_sec_max_90d
    FROM valid
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec_max_90d DESC, id) AS dedup_rank
        FROM scored
    ) ranked
    WHERE dedup_rank = 1
),
with_windows AS (
    SELECT
        id,
        page,
        duration_sec_max_90d,
        NTILE(3) OVER (ORDER BY next_duration_sec DESC, id) AS next_duration_sec_bucket,
        MAX(row_seq) OVER (PARTITION BY page ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS row_seq_max_30d,
        CUME_DIST() OVER (PARTITION BY page ORDER BY next_duration_sec DESC NULLS LAST) AS next_duration_sec_cume_dist,
        NTILE(5) OVER w_part AS duration_sec_max_90d_bucket,
        MIN(row_seq) OVER (PARTITION BY TO_DATE(event_time)) AS event_time_to_date_min_row_seq
    FROM ranked
    WHERE next_duration_sec BETWEEN 201 AND 422
    WINDOW w_part AS (ORDER BY duration_sec_max_90d DESC, id)
)
SELECT
    id,
    duration_sec_max_90d,
    next_duration_sec_bucket,
    next_duration_sec_cume_dist,
    duration_sec_max_90d_bucket,
    event_time_to_date_min_row_seq
FROM with_windows
ORDER BY duration_sec_max_90d DESC
