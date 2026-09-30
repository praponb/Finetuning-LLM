WITH kept_keys AS (
    SELECT session_id, id
    FROM web_events
    INTERSECT
    SELECT session_id, id
    FROM web_events
    WHERE event_time >= '2023-12-01'
),
verified AS (
    SELECT s.*
    FROM web_events s
    INNER JOIN kept_keys k ON s.session_id = k.session_id AND s.id = k.id
),
enriched AS (
    SELECT
        id,
        session_id,
        RANK() OVER (PARTITION BY session_id, event_type ORDER BY duration_sec DESC NULLS LAST) AS duration_sec_rank,
        DENSE_RANK() OVER (PARTITION BY event_type, session_id ORDER BY duration_sec DESC NULLS FIRST) AS duration_sec_dense_rank,
        COUNT(duration_sec) OVER (ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS duration_sec_count_7d
    FROM verified
),
cleaned AS (
    SELECT *, duration_sec_count_7d + duration_sec_dense_rank AS duration_sec_count_7d_plus_duration_sec_dense_rank
    FROM enriched
    WHERE duration_sec_dense_rank > 7
),
scored AS (
    SELECT
        *,
        ROUND(duration_sec_dense_rank / SUM(duration_sec_dense_rank) OVER (PARTITION BY session_id), 4) AS duration_sec_dense_rank_share,
        PERCENT_RANK() OVER w_hist AS duration_sec_count_7d_plus_duration_sec_dense_rank_percent_rank
    FROM cleaned
    WINDOW w_hist AS (ORDER BY duration_sec_count_7d_plus_duration_sec_dense_rank DESC NULLS LAST)
),
with_dims AS (
    SELECT
        src.*,
        s.country,
        s.device
    FROM scored src
    INNER JOIN sessions s
        ON src.session_id = s.id
)
SELECT
    s.id,
    s.session_id,
    s.duration_sec_rank,
    s.duration_sec_dense_rank,
    s.duration_sec_count_7d,
    s.duration_sec_dense_rank_share,
    s.device,
    CASE WHEN device RLIKE '^(tablet|desktop)$' THEN 'EXTENDED' WHEN device RLIKE '^(mobile)$' THEN 'PRIORITY' ELSE 'CORE' END AS device_zone,
    (SELECT MIN(x.duration_sec) FROM web_events x WHERE x.session_id = s.session_id) AS min_web_event_duration_sec
FROM with_dims s
ORDER BY duration_sec_dense_rank_share NULLS FIRST
