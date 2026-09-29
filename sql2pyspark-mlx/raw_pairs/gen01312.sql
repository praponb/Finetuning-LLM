WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        session_id,
        page,
        event_time,
        duration_sec
    FROM (
        SELECT *, RANK() OVER (PARTITION BY session_id ORDER BY event_time DESC NULLS LAST) AS latest_rank
        FROM web_events
    ) t
    WHERE latest_rank = 1
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time NULLS LAST, id) AS dedup_rank
    FROM deduped
    QUALIFY dedup_rank = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        session_id,
        page,
        event_time,
        duration_sec,
        dedup_rank,
        CUME_DIST() OVER (PARTITION BY page, session_id ORDER BY duration_sec NULLS LAST) AS duration_sec_cume_dist,
        NTILE(5) OVER (PARTITION BY page ORDER BY duration_sec DESC, id) AS duration_sec_bucket,
        LEAD(duration_sec, 1) OVER (PARTITION BY page ORDER BY duration_sec, id) AS next_duration_sec,
        AVG(duration_sec) OVER (ORDER BY event_time, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS duration_sec_avg_5,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY dedup_rank, id) AS event_seq
    FROM ranked
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(duration_sec) OVER (PARTITION BY page, session_id) AS page_max_duration_sec,
        MAX(next_duration_sec) OVER (PARTITION BY session_id ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS next_duration_sec_max_90d,
        NTILE(4) OVER (PARTITION BY YEAR(event_time), session_id ORDER BY duration_sec DESC, id) AS duration_sec_bucket_2
    FROM enriched
)
SELECT
    s.id,
    s.session_id,
    s.page,
    s.event_time,
    s.duration_sec,
    s.dedup_rank,
    s.duration_sec_cume_dist,
    s.duration_sec_bucket,
    s.next_duration_sec,
    (SELECT SUM(x.duration_sec) FROM web_events x WHERE x.session_id = s.session_id) AS sum_web_event_duration_sec
FROM metrics s
ORDER BY duration_sec_cume_dist DESC NULLS FIRST
