WITH base AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        moods
    FROM tracks
    WHERE bpm <= 142
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(duration_sec) OVER (PARTITION BY artist_id, is_explicit ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_duration_sec,
        duration_sec - LAG(duration_sec) OVER (PARTITION BY artist_id, is_explicit ORDER BY duration_sec, id) AS duration_sec_change,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER w_artist_id_is_explicit, 0), 4) AS duration_sec_share_of_artist_id_is_explicit,
        RANK() OVER w_artist_id_is_explicit_by_dur_3 AS duration_sec_rank
    FROM base
    WINDOW w_artist_id_is_explicit AS (PARTITION BY artist_id, is_explicit), w_artist_id_is_explicit_by_dur_3 AS (PARTITION BY artist_id, is_explicit ORDER BY duration_sec)
),
latest AS (
    -- Keep the earliest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        moods,
        first_duration_sec,
        duration_sec_change,
        duration_sec_share_of_artist_id_is_explicit,
        duration_sec_rank
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY first_duration_sec, id) AS row_num
        FROM sequenced
    ) ranked
    WHERE row_num = 1
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        LEAST(first_duration_sec, duration_sec_rank) AS least_first_duration_sec_duration_sec_rank,
        ARRAY_CONTAINS(moods, 'workout') AS has_workout
    FROM latest
    WHERE duration_sec BETWEEN 183 AND 352
),
qualified AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.bpm < 127
              AND t.release_date < DATE '2025-03-08'
        )
      AND s.is_explicit = TRUE
)
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    is_explicit,
    moods,
    first_duration_sec,
    duration_sec_change,
    duration_sec_rank,
    least_first_duration_sec_duration_sec_rank,
    has_workout,
    COALESCE(duration_sec_change, 0) AS duration_sec_change_filled
FROM qualified
ORDER BY duration_sec_change DESC, id
