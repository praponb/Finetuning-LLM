WITH recent_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm,
        moods,
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_label
    FROM tracks
    WHERE (is_explicit = FALSE OR artist_id IS NOT NULL)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        title,
        duration_sec,
        bpm,
        DENSE_RANK() OVER w_artist_id_by_duration_sec AS duration_sec_dense_rank,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER w_artist_id, 0), 4) AS duration_sec_share_of_artist_id
    FROM recent_tracks
    WINDOW w_artist_id_by_duration_sec AS (PARTITION BY artist_id ORDER BY duration_sec DESC, id), w_artist_id AS (PARTITION BY artist_id)
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        FLOOR(duration_sec_dense_rank / 2) * 2 AS duration_sec_dense_rank_bucket
    FROM ranked
    WHERE duration_sec BETWEEN 144 AND 392
),
with_activity AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM derived s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
        )
      AND s.duration_sec_share_of_artist_id > 0.3
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.duration_sec,
        s.duration_sec_dense_rank,
        s.duration_sec_share_of_artist_id,
        p.id AS play_id,
        p.listener_id AS play_listener_id,
        p.ms_played AS play_ms_played
    FROM with_activity s
    JOIN plays p ON p.track_id = s.id
),
filtered AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM with_children_plays s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.listener_id IS NOT NULL
        )
),
with_parent_listener AS (
    -- Join listeners
    SELECT
        s.id,
        s.duration_sec_dense_rank,
        s.play_id,
        s.play_listener_id,
        s.play_ms_played,
        l.birth_year AS listener_birth_year
    FROM filtered s
    LEFT JOIN listeners l ON l.id = s.play_listener_id
)
SELECT id, play_id, field, metric_value
FROM (
    SELECT id, play_id, play_ms_played, listener_birth_year
    FROM with_parent_listener
) src
UNPIVOT (
    metric_value FOR field IN (play_ms_played, listener_birth_year)
) unp
ORDER BY id, play_id, field
