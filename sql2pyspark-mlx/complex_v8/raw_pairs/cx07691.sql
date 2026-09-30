WITH recent_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label
    FROM plays
),
enriched_listener AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.is_skipped,
        s.source,
        s.is_skipped_label,
        l.country AS listener_country,
        l.username AS listener_username,
        l.signup_date AS listener_signup_date,
        t.release_date AS track_release_date,
        t.bpm AS track_bpm
    FROM recent_plays s
    LEFT JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(track_bpm) OVER (PARTITION BY is_skipped), 2) AS is_skipped_avg_track_bpm,
        LAST_VALUE(track_release_date) IGNORE NULLS OVER (ORDER BY track_release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_track_release_date
    FROM enriched_listener
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        source,
        is_skipped_label,
        track_release_date,
        track_bpm,
        is_skipped_avg_track_bpm,
        last_known_track_release_date,
        DATE_TRUNC('MONTH', listener_signup_date) AS listener_signup_date_month,
        CONCAT_WS(' / ', listener_country, is_skipped_label) AS listener_country_is_skipped_label_label
    FROM sequenced
    WHERE played_at >= TIMESTAMP '2025-01-13 14:00:00'
),
without_matches AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM tagged s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND NOT p.is_skipped
        )
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(track_bpm) OVER (PARTITION BY listener_id ORDER BY is_skipped_avg_track_bpm DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_track_bpm,
        MAX(is_skipped_avg_track_bpm) OVER (PARTITION BY listener_id ORDER BY is_skipped_avg_track_bpm DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS is_skipped_avg_track_bpm_max_2
    FROM without_matches
)
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    source,
    is_skipped_label,
    is_skipped_avg_track_bpm,
    last_known_track_release_date,
    listener_signup_date_month,
    listener_country_is_skipped_label_label,
    CONCAT_WS('|', source, is_skipped_label) AS source_is_skipped_label_label
FROM with_windows
ORDER BY is_skipped_label DESC, listener_country_is_skipped_label_label
