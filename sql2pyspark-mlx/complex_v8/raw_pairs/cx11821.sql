WITH prepared AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        LEAST(bpm, duration_sec) AS least_bpm_duration_sec,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug,
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_label
    FROM tracks
    WHERE NOT ARRAY_CONTAINS(moods, 'party')
      AND UPPER(TRIM(title)) LIKE 'M%'
),
qualified AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM prepared s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND SIZE(t.moods) >= 1
              AND UPPER(TRIM(t.title)) LIKE 'M%'
        )
),
joined_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.is_explicit,
        s.bpm,
        s.moods,
        s.least_bpm_duration_sec,
        s.title_slug,
        s.release_date_label,
        a.is_verified AS artist_is_verified,
        a.country AS artist_country,
        a.debut_year AS artist_debut_year
    FROM qualified s
    JOIN artists a ON a.id = s.artist_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(artist_debut_year, 2) OVER (PARTITION BY artist_is_verified ORDER BY least_bpm_duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_artist_debut_year,
        LAST_VALUE(bpm) OVER (PARTITION BY artist_country, artist_id ORDER BY artist_debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_bpm,
        FIRST_VALUE(least_bpm_duration_sec) IGNORE NULLS OVER (PARTITION BY artist_is_verified ORDER BY least_bpm_duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_least_bpm_duration_sec
    FROM joined_artist
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        is_explicit,
        moods,
        release_date_label,
        artist_country,
        artist_debut_year,
        last_bpm,
        first_known_least_bpm_duration_sec,
        DENSE_RANK() OVER (PARTITION BY artist_id ORDER BY artist_debut_year) AS artist_debut_year_dense_rank,
        SIZE(COLLECT_SET(artist_country) OVER (PARTITION BY artist_id, artist_is_verified)) AS distinct_artist_country_in_artist_id_artist_is_v,
        LEAD(artist_debut_year) OVER (PARTITION BY artist_id ORDER BY artist_debut_year, id) AS next_artist_debut_year
    FROM with_windows
),
exploded_detail_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_country,
        s.last_bpm,
        s.artist_debut_year_dense_rank,
        s.distinct_artist_country_in_artist_id_artist_is_v,
        s.next_artist_debut_year,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.device AS play_device,
        p.is_skipped AS play_is_skipped
    FROM ranked s
    JOIN plays p ON p.track_id = s.id
),
filtered AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM exploded_detail_plays s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.listener_id IS NOT NULL
              AND p.device = 'phone'
        )
      AND s.play_is_skipped = FALSE
)
SELECT
    id,
    artist_debut_year_dense_rank,
    distinct_artist_country_in_artist_id_artist_is_v,
    play_ms_played,
    play_device
FROM filtered
ORDER BY artist_debut_year_dense_rank, play_device
