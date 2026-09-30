WITH base_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown
    FROM plays
    WHERE is_skipped = TRUE
      AND device = 'tv'
),
with_details AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.is_skipped,
        s.source_or_unknown,
        l.country AS listener_country,
        l.interests AS listener_interests
    FROM base_plays s
    JOIN listeners l ON l.id = s.listener_id AND UPPER(TRIM(l.username)) LIKE 'H%'
    WHERE l.country IN ('KR', 'UK')
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        played_at,
        is_skipped,
        source_or_unknown,
        listener_country,
        ROUND(PERCENT_RANK() OVER (PARTITION BY track_id ORDER BY ms_played DESC, id), 3) AS ms_played_percent_rank,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER w_listener_id, 0), 4) AS ms_played_share_of_listener_id,
        FIRST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY listener_id, track_id ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played,
        ROUND(CUME_DIST() OVER (PARTITION BY listener_id, track_id ORDER BY ms_played DESC), 3) AS ms_played_cume_dist,
        MAX(ms_played) OVER (PARTITION BY listener_id) AS listener_id_max_ms_played,
        COUNT(*) OVER w_listener_id AS rows_in_listener_id,
        SUM(ms_played) OVER w_listener_id AS listener_id_sum_ms_played,
        ROW_NUMBER() OVER (PARTITION BY track_id ORDER BY ms_played DESC, id) AS rn,
        ROUND(PERCENT_RANK() OVER (PARTITION BY track_id ORDER BY ms_played DESC), 3) AS ms_played_percent_rank_2,
        ROUND(CUME_DIST() OVER w_listener_id_track_id_by_ms_p_3, 3) AS ms_played_cume_dist_2
    FROM with_details
    WINDOW w_listener_id AS (PARTITION BY listener_id), w_listener_id_track_id_by_ms_p_3 AS (PARTITION BY listener_id, track_id ORDER BY ms_played DESC, id)
)
-- Final projection
SELECT
    id,
    played_at,
    is_skipped,
    source_or_unknown,
    ms_played_share_of_listener_id,
    ms_played_cume_dist,
    listener_id_max_ms_played,
    rows_in_listener_id,
    listener_id_sum_ms_played,
    rn,
    ms_played_percent_rank_2,
    ms_played_cume_dist_2,
    LOWER(REGEXP_REPLACE(TRIM(listener_country), ' ', '_')) AS listener_country_slug
FROM metrics
ORDER BY played_at DESC, id
LIMIT 5
