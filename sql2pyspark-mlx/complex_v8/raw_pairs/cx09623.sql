WITH prepared_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE artist_id IS NOT NULL
      AND UPPER(TRIM(title)) LIKE 'E%'
),
without_matches AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM prepared_tracks s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
        )
),
deduped AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        is_explicit,
        bpm,
        moods
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY bpm DESC, id DESC) AS rn
        FROM without_matches
    ) ranked
    WHERE rn = 1
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        artist_id,
        is_explicit,
        bpm,
        moods,
        CASE WHEN bpm >= 142 THEN 'A' WHEN bpm >= 96 THEN 'B' ELSE 'C' END AS bpm_tier
    FROM deduped
    WHERE NOT ARRAY_CONTAINS(moods, 'workout')
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        bpm_tier,
        artist_id,
        COUNT(DISTINCT artist_id) AS distinct_artist_ids,
        COUNT(DISTINCT bpm_tier) AS distinct_bpm_tiers,
        COUNT(*) AS n_rows
    FROM labeled
    WHERE artist_id IS NOT NULL
    GROUP BY 1, 2
    HAVING COUNT(*) >= 2
),
detail_rows AS (
    -- One row per track
    SELECT
        s.bpm_tier,
        s.artist_id,
        s.distinct_bpm_tiers,
        s.n_rows,
        t.id AS track_id,
        t.bpm AS track_bpm,
        t.moods AS track_moods,
        t.duration_sec AS track_duration_sec
    FROM rollup_base s
    LEFT JOIN tracks t ON t.artist_id = s.artist_id
)
SELECT
    bpm_tier,
    distinct_bpm_tiers,
    n_rows,
    track_id,
    track_bpm,
    track_duration_sec,
    ARRAY_CONTAINS(track_moods, 'party') AS has_party,
    GREATEST(n_rows, track_duration_sec) AS greatest_n_rows_track_duration_sec
FROM detail_rows
ORDER BY greatest_n_rows_track_duration_sec DESC NULLS FIRST, n_rows
