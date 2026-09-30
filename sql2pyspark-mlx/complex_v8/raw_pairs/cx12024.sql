WITH filtered AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE ms_played >= 108979
      AND is_skipped = FALSE
),
combined_details AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        t.bpm AS track_bpm,
        t.duration_sec AS track_duration_sec,
        a.country AS artist_country,
        a.debut_year AS artist_debut_year
    FROM filtered s
    JOIN tracks t ON t.id = s.track_id
    LEFT JOIN artists a ON a.id = t.artist_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        track_bpm - LAG(track_bpm) OVER (ORDER BY track_duration_sec DESC, id) AS track_bpm_change,
        DENSE_RANK() OVER (ORDER BY track_duration_sec DESC) AS track_duration_sec_dense_rank,
        LAG(played_at) OVER (PARTITION BY MONTH(played_at), device ORDER BY artist_debut_year DESC, id) AS prev_played_at,
        COUNT(*) OVER w_listener_id_device AS rows_in_listener_id_device,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY MONTH(played_at)), 2) AS ms_played_vs_played_at_month_avg,
        ROUND(AVG(ms_played) OVER (PARTITION BY listener_id, device), 2) AS listener_id_device_avg_ms_played,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY listener_id, device)) AS distinct_track_id_in_listener_id_device
    FROM combined_details
    WINDOW w_listener_id_device AS (PARTITION BY listener_id, device)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        artist_country,
        listener_id,
        DATE_TRUNC('MONTH', played_at) AS played_at_month,
        ARRAY_SORT(COLLECT_SET(device)) AS device_list,
        MIN(prev_played_at) AS first_prev_played_at,
        ROUND(AVG(track_duration_sec_dense_rank), 2) AS avg_track_duration_sec_dense_rank
    FROM sequenced
    WHERE device = 'car'
    GROUP BY artist_country, listener_id, DATE_TRUNC('MONTH', played_at)
    HAVING SUM(artist_debut_year) > 1994
)
SELECT
    artist_country,
    listener_id,
    played_at_month,
    device_list,
    CONCAT(artist_country, '#', CAST(listener_id AS STRING)) AS artist_country_ref,
    LOWER(REGEXP_REPLACE(TRIM(artist_country), ' ', '_')) AS artist_country_slug
FROM rollup_base
ORDER BY artist_country, artist_country_ref DESC, listener_id, played_at_month
LIMIT 20
