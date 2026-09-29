WITH recent_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        SUBSTRING(TRIM(title), 1, 4) AS title_prefix
    FROM tracks
    WHERE LENGTH(TRIM(title)) >= 5
),
combined_details_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.moods,
        s.title_prefix,
        a.is_verified AS artist_is_verified
    FROM recent_tracks s
    JOIN artists a ON a.id = s.artist_id
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        artist_is_verified,
        artist_id,
        SUM(CASE WHEN NOT artist_is_verified THEN 1 ELSE 0 END) AS n_not_artist_is_verified,
        SUM(CASE WHEN artist_is_verified = FALSE THEN 1 ELSE 0 END) AS n_artist_is_verified_false,
        COUNT(*) FILTER (WHERE NOT artist_is_verified) AS n_not_artist_is_verified_2,
        SUM(CASE WHEN artist_is_verified = TRUE THEN 1 ELSE 0 END) AS n_artist_is_verified_true,
        COUNT(*) FILTER (WHERE artist_is_verified = FALSE) AS n_artist_is_verified_false_2
    FROM combined_details_artist
    GROUP BY artist_is_verified, artist_id
)
SELECT
    artist_is_verified,
    n_not_artist_is_verified,
    n_not_artist_is_verified_2,
    n_artist_is_verified_false_2,
    CASE WHEN n_not_artist_is_verified >= 8 THEN 'HIGH' WHEN n_not_artist_is_verified >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS n_not_artist_is_verified_tier
FROM per_group
ORDER BY n_artist_is_verified_false_2, n_not_artist_is_verified
