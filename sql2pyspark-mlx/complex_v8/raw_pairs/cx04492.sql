WITH recent_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE ms_played > 95960
),
combined_details AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.is_skipped,
        t.artist_id AS track_artist_id,
        a.debut_year AS artist_debut_year
    FROM recent_plays s
    JOIN tracks t ON t.id = s.track_id
    LEFT JOIN artists a ON a.id = t.artist_id
    WHERE t.artist_id IS NOT NULL
),
with_details_artist AS (
    -- Join artists
    SELECT s.*, a.is_verified AS artist_is_verified
    FROM combined_details s
    LEFT JOIN artists a ON a.id = s.track_artist_id
)
SELECT
    track_artist_id,
    is_skipped,
    listener_id,
    GROUPING(track_artist_id) AS is_total_track_artist_id,
    COUNT(*) AS cnt,
    SUM(artist_debut_year) AS total_artist_debut_year,
    COUNT(DISTINCT listener_id) AS distinct_listener_ids
FROM with_details_artist
GROUP BY GROUPING SETS ((track_artist_id, is_skipped, listener_id), (track_artist_id))
