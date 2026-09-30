WITH scoped AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        ADD_MONTHS(release_date, 12) AS release_date_plus_12m,
        SIZE(moods) AS moods_count,
        DAYOFWEEK(release_date) AS release_date_dayofweek
    FROM tracks
    WHERE LENGTH(TRIM(title)) >= 6
      AND bpm BETWEEN 75 AND 170
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        LEAST(duration_sec, release_date_dayofweek) AS least_duration_sec_release_date_dayofweek
    FROM scoped
    WHERE release_date_dayofweek > 3
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM tagged
    WHERE duration_sec < (SELECT MAX(duration_sec) FROM tagged)
),
screened AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM with_activity s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.source IN ('album', 'playlist')
        )
)
SELECT id, measure, amount
FROM (
    SELECT id, duration_sec, least_duration_sec_release_date_dayofweek
    FROM screened
) src
UNPIVOT (
    amount FOR measure IN (duration_sec, least_duration_sec_release_date_dayofweek)
) unp
