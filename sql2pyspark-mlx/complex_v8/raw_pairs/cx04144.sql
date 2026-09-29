WITH valid AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        DATE_FORMAT(played_at, 'MMM yyyy') AS played_at_label
    FROM plays
    WHERE ms_played >= 51883
),
enriched_track AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.source,
        s.played_at_label,
        t.release_date AS track_release_date
    FROM valid s
    LEFT JOIN tracks t ON t.id = s.track_id
),
with_details_track AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.source,
        s.played_at_label,
        s.track_release_date,
        t.release_date AS track_release_date_2,
        t.moods AS track_moods,
        l.country AS listener_country
    FROM enriched_track s
    JOIN tracks t ON t.id = s.track_id
    LEFT JOIN listeners l ON l.id = s.listener_id
),
latest AS (
    -- Keep the latest row per track_id, source
    SELECT
        id,
        listener_id,
        track_id,
        source,
        played_at_label,
        track_release_date,
        track_release_date_2,
        track_moods,
        listener_country
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY track_id, source ORDER BY track_release_date_2 DESC, id DESC) AS rn
        FROM with_details_track
    ) ranked
    WHERE rn = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        track_id,
        source,
        played_at_label,
        track_release_date,
        track_release_date_2,
        track_moods,
        listener_country,
        NTILE(10) OVER (PARTITION BY listener_country ORDER BY track_release_date NULLS LAST, id) AS track_release_date_decile,
        LEAD(track_release_date_2, 3) OVER (PARTITION BY track_id, listener_id ORDER BY track_release_date_2, id) AS next3_track_release_date_2,
        LAG(track_release_date_2) OVER w_track_id_listener_id_by_trac AS prev_track_release_date_2
    FROM latest
    WINDOW w_track_id_listener_id_by_trac AS (PARTITION BY track_id, listener_id ORDER BY track_release_date_2, id)
),
with_details_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.source,
        s.played_at_label,
        s.track_release_date,
        s.track_moods,
        s.listener_country,
        s.track_release_date_decile,
        l.username AS listener_username,
        l.interests AS listener_interests,
        l.birth_year AS listener_birth_year
    FROM with_windows s
    JOIN listeners l ON l.id = s.listener_id
),
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.source,
        s.track_moods,
        s.listener_country,
        s.listener_username,
        p.id AS play_id,
        p.source AS play_source,
        p.ms_played AS play_ms_played
    FROM with_details_listener s
    JOIN plays p ON p.track_id = s.track_id AND p.ms_played < 290754
)
-- Final projection
SELECT
    id,
    track_id,
    source,
    track_moods,
    listener_country,
    play_id,
    play_source,
    play_ms_played
FROM detail_rows_plays
ORDER BY id DESC
