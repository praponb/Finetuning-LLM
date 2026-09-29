WITH base AS (
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
        LOWER(TRIM(device)) AS device_clean,
        played_at + INTERVAL 15 MINUTES AS played_at_plus_15m,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown
    FROM plays
    WHERE ms_played > 176302
),
filtered AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.device <> 'tv'
        )
),
enriched AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.is_skipped,
        s.device_clean,
        s.played_at_plus_15m,
        s.source_or_unknown,
        t.title AS track_title,
        t.artist_id AS track_artist_id,
        l.signup_date AS listener_signup_date
    FROM filtered s
    LEFT JOIN tracks t ON t.id = s.track_id
    JOIN listeners l ON l.id = s.listener_id
),
stats AS (
    -- Group-level totals
    SELECT
        listener_id,
        ROUND(100 * COUNT(CASE WHEN UPPER(TRIM(track_title)) LIKE 'G%' THEN 1 END) / COUNT(*), 1) AS pct_upper_trim_track_title_like,
        COUNT(*) AS cnt,
        ROUND(100 * COUNT(CASE WHEN NOT is_skipped THEN 1 END) / COUNT(*), 1) AS pct_not_is_skipped
    FROM enriched
    GROUP BY 1
),
with_details_listener AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        l.interests AS listener_interests,
        l.username AS listener_username,
        l.country AS listener_country
    FROM stats s
    JOIN listeners l ON l.id = s.listener_id AND l.plan = 'free'
),
screened AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM with_details_listener s
    WHERE s.listener_id IN (
            SELECT p.listener_id
            FROM plays p
            WHERE p.source = 'playlist'
        )
      AND s.pct_not_is_skipped <= 90.0
),
exploded AS (
    -- One row per listener_interest
    SELECT
        s.listener_id,
        s.pct_upper_trim_track_title_like,
        s.cnt,
        s.pct_not_is_skipped,
        s.listener_username,
        s.listener_country,
        t.listener_interest
    FROM screened s
    LATERAL VIEW EXPLODE(s.listener_interests) t AS listener_interest
)
SELECT
    listener_id,
    pct_not_is_skipped,
    listener_username,
    listener_interest,
    SUBSTRING(TRIM(listener_interest), 1, 2) AS listener_interest_prefix,
    cnt + pct_not_is_skipped AS cnt_plus_pct_not_is_skipped
FROM exploded
ORDER BY cnt_plus_pct_not_is_skipped, listener_interest
