WITH valid AS (
    -- Keep valid rows and derive helper columns
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
    WHERE TO_DATE(played_at) < DATE '2025-03-02'
      AND ms_played > 129336
),
joined AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        t.title AS track_title,
        a.is_verified AS artist_is_verified,
        a.debut_year AS artist_debut_year
    FROM valid s
    LEFT JOIN tracks t ON t.id = s.track_id
    JOIN artists a ON a.id = t.artist_id
),
with_activity AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM joined s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.device IN ('speaker', 'tv', 'car')
              AND NOT p.is_skipped
        )
),
with_details AS (
    -- Attach listener attributes
    SELECT
        s.*,
        l.signup_date AS listener_signup_date,
        l.birth_year AS listener_birth_year,
        l.username AS listener_username,
        t.release_date AS track_release_date
    FROM with_activity s
    JOIN listeners l ON l.id = s.listener_id
    JOIN tracks t ON t.id = s.track_id
    WHERE LOWER(l.username) LIKE '%.s%'
),
without_matches AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM with_details s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.played_at >= TIMESTAMP '2025-01-31 12:00:00'
        )
      AND s.listener_signup_date BETWEEN DATE '2024-08-02' AND DATE '2025-07-08'
),
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.track_title,
        s.artist_debut_year,
        s.listener_signup_date,
        s.listener_username,
        s.track_release_date,
        p.id AS play_id,
        p.is_skipped AS play_is_skipped,
        p.track_id AS play_track_id,
        p.played_at AS play_played_at,
        p.ms_played AS play_ms_played
    FROM without_matches s
    JOIN plays p ON p.listener_id = s.listener_id
)
SELECT id, play_id, field, amount
FROM (
    SELECT id, play_id, artist_debut_year, ms_played, play_ms_played
    FROM detail_rows_plays
) src
UNPIVOT (
    amount FOR field IN (artist_debut_year AS m_artist_debut_year, ms_played AS ms_played_v, play_ms_played AS m_play_ms_played)
) unp
