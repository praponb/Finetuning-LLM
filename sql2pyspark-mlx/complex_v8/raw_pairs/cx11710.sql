WITH filtered_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE is_explicit = TRUE
      AND UPPER(TRIM(title)) LIKE 'M%'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        bpm - LAG(bpm) OVER (PARTITION BY is_explicit ORDER BY duration_sec NULLS LAST, id) AS bpm_change,
        SUM(bpm) OVER (PARTITION BY is_explicit, artist_id) AS is_explicit_artist_id_sum_bpm,
        RANK() OVER (PARTITION BY is_explicit ORDER BY duration_sec NULLS LAST) AS duration_sec_rank,
        SUM(duration_sec) OVER (PARTITION BY is_explicit ORDER BY duration_sec NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec
    FROM filtered_tracks
),
qualified AS (
    -- Keep rows whose artist matches
    SELECT s.*
    FROM sequenced s
    WHERE NOT EXISTS (
            SELECT 1
            FROM artists a
            WHERE a.id = s.artist_id
              AND UPPER(TRIM(a.artist_name)) LIKE 'K%'
        )
),
joined_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.is_explicit,
        s.bpm,
        s.moods,
        s.bpm_change,
        s.is_explicit_artist_id_sum_bpm,
        s.duration_sec_rank,
        s.running_duration_sec,
        a.is_verified AS artist_is_verified
    FROM qualified s
    JOIN artists a ON a.id = s.artist_id
    WHERE NOT a.is_verified
)
SELECT id, field, value
FROM (
    SELECT id, bpm, bpm_change
    FROM joined_artist
) src
UNPIVOT (
    value FOR field IN (bpm, bpm_change)
) unp
