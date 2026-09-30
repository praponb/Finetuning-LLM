WITH valid AS (
    -- Scope the plays rows
    SELECT *
    FROM plays
    WHERE played_at < TIMESTAMP '2025-02-18 10:00:00'
),
combined_details_track AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.is_skipped,
        s.source,
        t.release_date AS track_release_date
    FROM valid s
    LEFT JOIN tracks t ON t.id = s.track_id
),
combined_details_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.track_release_date,
        l.country AS listener_country,
        l.birth_year AS listener_birth_year,
        l.username AS listener_username
    FROM combined_details_track s
    JOIN listeners l ON l.id = s.listener_id AND l.birth_year <= 1995
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(ms_played / NULLIF(listener_birth_year, 0), 3) AS ms_played_per_listener_birth_year,
        ROUND(listener_birth_year / NULLIF(ms_played, 0), 3) AS listener_birth_year_per_ms_played
    FROM combined_details_listener
    WHERE track_id >= 14
),
listener_id_stats AS (
    -- Median listener_birth_year per listener_id
    SELECT listener_id, PERCENTILE_APPROX(listener_birth_year, 0.5) AS median_listener_birth_year
    FROM enriched
    GROUP BY listener_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_listener_birth_year,
        ROUND(s.listener_birth_year - g.median_listener_birth_year, 2) AS listener_birth_year_minus_median
    FROM enriched s
    JOIN listener_id_stats g ON g.listener_id = s.listener_id
    WHERE s.listener_birth_year >= g.median_listener_birth_year
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    ms_played,
    track_release_date,
    listener_birth_year,
    listener_username,
    ms_played_per_listener_birth_year,
    listener_birth_year_per_ms_played,
    listener_birth_year_minus_median
FROM above_average
WHERE median_listener_birth_year >= 1974.4
ORDER BY listener_birth_year_minus_median NULLS LAST, track_release_date DESC NULLS FIRST
