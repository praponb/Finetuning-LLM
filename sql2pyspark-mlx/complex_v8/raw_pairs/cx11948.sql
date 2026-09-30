WITH valid AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown
    FROM plays
    WHERE (is_skipped = TRUE OR ms_played < 326250)
),
enriched_track AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.is_skipped,
        s.source,
        s.source_or_unknown,
        s.device_or_unknown,
        t.bpm AS track_bpm,
        t.artist_id AS track_artist_id
    FROM valid s
    JOIN tracks t ON t.id = s.track_id
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        track_bpm,
        track_artist_id,
        CASE WHEN track_bpm >= 143 THEN 'HIGH' WHEN track_bpm >= 94 THEN 'MEDIUM' ELSE 'LOW' END AS track_bpm_tier,
        LOWER(REGEXP_REPLACE(TRIM(source_or_unknown), ' ', '_')) AS source_or_unknown_slug
    FROM enriched_track
),
enriched_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.is_skipped,
        s.source,
        s.track_artist_id,
        s.track_bpm_tier,
        s.source_or_unknown_slug,
        a.artist_name,
        a.genre AS artist_genre,
        a.debut_year AS artist_debut_year,
        l.signup_date AS listener_signup_date,
        l.birth_year AS listener_birth_year
    FROM enriched s
    JOIN artists a ON a.id = s.track_artist_id
    LEFT JOIN listeners l ON l.id = s.listener_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY listener_signup_date, id), 3) AS listener_signup_date_cume_dist,
        listener_birth_year - LAG(listener_birth_year) OVER (ORDER BY listener_signup_date, id) AS listener_birth_year_change
    FROM enriched_artist
),
deduped AS (
    -- Keep the latest row per artist_genre, source
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        track_artist_id,
        track_bpm_tier,
        source_or_unknown_slug,
        artist_name,
        artist_genre,
        artist_debut_year,
        listener_signup_date,
        listener_birth_year,
        listener_signup_date_cume_dist,
        listener_birth_year_change
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_genre, source ORDER BY listener_signup_date_cume_dist DESC, id DESC) AS recency_rank
        FROM metrics
    ) ranked
    WHERE recency_rank = 1
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN p.is_skipped = FALSE THEN 1 END) FROM plays p WHERE p.track_id = s.track_id) AS play_is_skipped_count,
        (SELECT MAX(p.ms_played) FROM plays p WHERE p.track_id = s.track_id) AS max_play_ms_played
    FROM deduped s
)
SELECT id, metric_name, amount
FROM (
    SELECT id, artist_debut_year, listener_birth_year_change, listener_birth_year
    FROM summarized_rows
) src
UNPIVOT (
    amount FOR metric_name IN (artist_debut_year, listener_birth_year_change, listener_birth_year)
) unp
