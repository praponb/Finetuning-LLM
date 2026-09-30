WITH valid AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        YEAR(played_at) AS played_at_year
    FROM plays
    WHERE track_id IS NOT NULL
      AND source IN ('playlist', 'radio', 'search')
),
device_stats AS (
    -- Avg played_at_year per device
    SELECT device, AVG(played_at_year) AS avg_played_at_year
    FROM valid
    GROUP BY device
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_played_at_year,
        ROUND(s.played_at_year - g.avg_played_at_year, 2) AS played_at_year_minus_avg
    FROM valid s
    JOIN device_stats g ON g.device = s.device
    WHERE s.played_at_year > g.avg_played_at_year
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN avg_played_at_year >= 2025.4 THEN 'HIGH' WHEN avg_played_at_year >= 2024.9 THEN 'MEDIUM' ELSE 'LOW' END AS avg_played_at_year_tier,
        played_at + INTERVAL 15 MINUTES AS played_at_plus_15m,
        GREATEST(played_at_year, avg_played_at_year) AS greatest_played_at_year_avg_played_at_year
    FROM compared
),
deduped AS (
    -- Keep the latest row per avg_played_at_year_tier, listener_id
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        played_at_year,
        avg_played_at_year,
        played_at_year_minus_avg,
        avg_played_at_year_tier,
        played_at_plus_15m,
        greatest_played_at_year_avg_played_at_year
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY avg_played_at_year_tier, listener_id ORDER BY played_at_year DESC, id DESC) AS row_num
        FROM enriched
    ) ranked
    WHERE row_num = 1
),
with_details_track AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.is_skipped,
        s.source,
        s.played_at_year,
        s.avg_played_at_year,
        s.played_at_year_minus_avg,
        s.avg_played_at_year_tier,
        s.played_at_plus_15m,
        s.greatest_played_at_year_avg_played_at_year,
        t.is_explicit AS track_is_explicit,
        t.moods AS track_moods
    FROM deduped s
    JOIN tracks t ON t.id = s.track_id
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        played_at_year,
        avg_played_at_year,
        played_at_year_minus_avg,
        played_at_plus_15m,
        greatest_played_at_year_avg_played_at_year,
        FLOOR(played_at_year_minus_avg / 1000) * 1000 AS played_at_year_minus_avg_bucket
    FROM with_details_track
    WHERE track_is_explicit = TRUE
),
with_parent_track AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.is_skipped,
        s.played_at_year,
        s.avg_played_at_year,
        s.played_at_year_minus_avg,
        s.greatest_played_at_year_avg_played_at_year,
        s.played_at_year_minus_avg_bucket,
        t.moods AS track_moods,
        t.is_explicit AS track_is_explicit,
        l.signup_date AS listener_signup_date
    FROM annotated s
    JOIN tracks t ON t.id = s.track_id
    JOIN listeners l ON l.id = s.listener_id
    WHERE NOT t.is_explicit
)
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    is_skipped,
    played_at_year,
    avg_played_at_year,
    played_at_year_minus_avg,
    played_at_year_minus_avg_bucket,
    track_moods,
    listener_signup_date
FROM with_parent_track
WHERE greatest_played_at_year_avg_played_at_year < 2025.8
ORDER BY played_at DESC NULLS FIRST, id
LIMIT 10
