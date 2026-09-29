WITH base AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source,
        MONTH(played_at) AS played_at_month
    FROM plays
    WHERE (ms_played > 73625 OR played_at < TIMESTAMP '2025-02-18 10:00:00')
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.source,
        s.played_at_month,
        t.title AS track_title,
        t.is_explicit AS track_is_explicit,
        t.artist_id AS track_artist_id
    FROM base s
    LEFT JOIN tracks t ON t.id = s.track_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        track_id,
        device,
        source,
        played_at_month,
        track_title,
        track_is_explicit,
        track_artist_id,
        MIN(played_at_month) OVER w_track_is_explicit_device AS track_is_explicit_device_min_played_at_month,
        ROUND(CUME_DIST() OVER (PARTITION BY device ORDER BY played_at_month ASC), 3) AS played_at_month_cume_dist,
        ROUND(played_at_month / NULLIF(SUM(played_at_month) OVER w_track_is_explicit_device, 0), 4) AS played_at_month_share_of_track_is_explicit_devic,
        ROUND(PERCENT_RANK() OVER (PARTITION BY source, track_artist_id ORDER BY played_at_month DESC), 3) AS played_at_month_percent_rank,
        DENSE_RANK() OVER w_device_by_played_at_month AS played_at_month_dense_rank,
        NTH_VALUE(played_at_month, 2) OVER (PARTITION BY source, track_artist_id ORDER BY played_at_month DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at_month,
        ROUND(played_at_month - AVG(played_at_month) OVER w_track_is_explicit_device, 2) AS played_at_month_vs_track_is_explicit_device_avg,
        MAX(played_at_month) OVER (PARTITION BY track_is_explicit, device) AS track_is_explicit_device_max_played_at_month,
        COUNT(*) OVER (PARTITION BY track_is_explicit, device) AS rows_in_track_is_explicit_device
    FROM enriched
    WINDOW w_track_is_explicit_device AS (PARTITION BY track_is_explicit, device), w_device_by_played_at_month AS (PARTITION BY device ORDER BY played_at_month ASC, id)
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        INITCAP(TRIM(device)) AS device_clean,
        ROUND(played_at_month / NULLIF(second_played_at_month, 0), 3) AS played_at_month_per_second_played_at_month,
        IF(track_is_explicit, 'yes', 'no') AS track_is_explicit_label
    FROM with_trends
    WHERE device = 'car'
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        track_id,
        source,
        played_at_month,
        track_title,
        track_artist_id,
        track_is_explicit_device_min_played_at_month,
        played_at_month_cume_dist,
        played_at_month_share_of_track_is_explicit_devic,
        played_at_month_percent_rank,
        played_at_month_dense_rank,
        second_played_at_month,
        played_at_month_vs_track_is_explicit_device_avg,
        track_is_explicit_device_max_played_at_month,
        device_clean,
        track_is_explicit_label,
        ROUND(AVG(played_at_month_percent_rank) OVER (PARTITION BY track_is_explicit_label), 2) AS track_is_explicit_label_avg_played_at_month_perc,
        LEAD(second_played_at_month) OVER (PARTITION BY source ORDER BY track_is_explicit_device_max_played_at_month DESC NULLS FIRST, id) AS next_second_played_at_month,
        SIZE(COLLECT_SET(track_artist_id) OVER w_track_is_explicit_label) AS distinct_track_artist_id_in_track_is_explicit_la,
        ROUND(PERCENT_RANK() OVER w_source_by_track_is_explicit_2, 3) AS track_is_explicit_device_max_played_at_month_per,
        LEAD(played_at_month_percent_rank) OVER (PARTITION BY source ORDER BY track_is_explicit_device_max_played_at_month DESC NULLS FIRST, id) AS next_played_at_month_percent_rank
    FROM tagged
    WINDOW w_track_is_explicit_label AS (PARTITION BY track_is_explicit_label), w_source_by_track_is_explicit_2 AS (PARTITION BY source ORDER BY track_is_explicit_device_max_played_at_month DESC NULLS FIRST)
)
-- Final projection
SELECT
    id,
    track_id,
    source,
    played_at_month,
    track_artist_id,
    track_is_explicit_device_min_played_at_month,
    played_at_month_cume_dist,
    played_at_month_share_of_track_is_explicit_devic,
    played_at_month_percent_rank,
    played_at_month_dense_rank,
    second_played_at_month,
    played_at_month_vs_track_is_explicit_device_avg,
    track_is_explicit_device_max_played_at_month,
    device_clean,
    track_is_explicit_label_avg_played_at_month_perc,
    distinct_track_artist_id_in_track_is_explicit_la,
    track_is_explicit_device_max_played_at_month_per,
    COALESCE(second_played_at_month, 0) AS second_played_at_month_filled,
    LOWER(REGEXP_REPLACE(TRIM(device_clean), ' ', '_')) AS device_clean_slug
FROM windowed
ORDER BY track_is_explicit_device_max_played_at_month DESC, track_artist_id, id
LIMIT 10
