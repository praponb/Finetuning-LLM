WITH recent_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        moods
    FROM tracks
),
qualified AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM recent_tracks s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.is_skipped = TRUE
              AND p.played_at < TIMESTAMP '2025-02-24 11:00:00'
        )
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label,
        DATEDIFF(DATE '2025-12-31', release_date) AS days_since_release_date,
        ABS(duration_sec - 344) AS duration_sec_distance
    FROM qualified
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM tagged
    WHERE duration_sec > (SELECT AVG(duration_sec) FROM tagged)
),
latest_per_key AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        is_explicit_label,
        days_since_release_date,
        duration_sec_distance
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY duration_sec_distance DESC, id DESC) AS row_num
        FROM without_matches
    ) ranked
    WHERE row_num = 1
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        release_date,
        is_explicit,
        is_explicit_label,
        days_since_release_date,
        DATE_TRUNC('WEEK', release_date) AS release_date_week,
        TRUNC(release_date, 'MM') AS release_date_month_start,
        LEAST(duration_sec, days_since_release_date) AS least_duration_sec_days_since_release_date
    FROM latest_per_key
)
SELECT *
FROM (
    SELECT release_date_month_start, is_explicit_label, days_since_release_date
    FROM labeled
) src
PIVOT (
    MAX(days_since_release_date)
    FOR is_explicit_label IN ('no', 'yes')
)
ORDER BY release_date_month_start
