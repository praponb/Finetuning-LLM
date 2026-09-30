WITH filtered_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        CONCAT_WS('|', source, device) AS source_device_label,
        LOWER(TRIM(source)) AS source_clean
    FROM plays
    WHERE HOUR(played_at) BETWEEN 8 AND 14
),
with_parent AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.source_device_label,
        s.source_clean,
        l.birth_year AS listener_birth_year
    FROM filtered_plays s
    JOIN listeners l ON l.id = s.listener_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        played_at,
        device,
        source_device_label,
        listener_birth_year,
        ROUND(listener_birth_year / NULLIF(SUM(listener_birth_year) OVER (PARTITION BY listener_id), 0), 4) AS listener_birth_year_share_of_listener_id,
        ROUND(AVG(listener_birth_year) OVER (PARTITION BY listener_id, track_id), 2) AS listener_id_track_id_avg_listener_birth_year,
        listener_birth_year - LAG(listener_birth_year) OVER (PARTITION BY listener_id, device ORDER BY listener_birth_year DESC NULLS FIRST, id) AS listener_birth_year_change,
        LAG(listener_birth_year, 3) OVER w_all_by_played_at AS prev3_listener_birth_year,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY listener_id)) AS distinct_track_id_in_listener_id,
        MIN(listener_birth_year) OVER (PARTITION BY listener_id) AS listener_id_min_listener_birth_year,
        ROUND(listener_birth_year - AVG(listener_birth_year) OVER (PARTITION BY listener_id, track_id), 2) AS listener_birth_year_vs_listener_id_track_id_avg,
        LAG(listener_birth_year) OVER (ORDER BY listener_birth_year DESC, id) AS prev_listener_birth_year
    FROM with_parent
    WINDOW w_all_by_played_at AS (ORDER BY played_at NULLS LAST, id)
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        played_at + INTERVAL 2 HOURS AS played_at_plus_2h,
        ROUND(prev_listener_birth_year / NULLIF(listener_birth_year, 0), 3) AS prev_listener_birth_year_per_listener_birth_year,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown
    FROM with_trends
),
totals AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_ym,
        DATE_FORMAT(played_at_plus_2h, 'yyyy-MM') AS played_at_plus_2h_ym,
        distinct_track_id_in_listener_id,
        COUNT(DISTINCT listener_id) AS distinct_listener_ids,
        MIN(listener_id_track_id_avg_listener_birth_year) AS min_listener_id_track_id_avg_listener_birth_year
    FROM annotated
    GROUP BY DATE_FORMAT(played_at, 'yyyy-MM'), DATE_FORMAT(played_at_plus_2h, 'yyyy-MM'), distinct_track_id_in_listener_id
)
-- Final projection
SELECT
    played_at_ym,
    played_at_plus_2h_ym,
    distinct_listener_ids,
    CASE WHEN distinct_listener_ids >= 9 THEN 'HIGH' WHEN distinct_listener_ids >= 5 THEN 'MEDIUM' ELSE 'LOW' END AS distinct_listener_ids_tier,
    FLOOR(distinct_listener_ids / 2) * 2 AS distinct_listener_ids_bucket
FROM totals
ORDER BY distinct_listener_ids_tier DESC, played_at_ym NULLS LAST
