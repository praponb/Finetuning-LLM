WITH valid_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        CONCAT(source, '#', CAST(track_id AS STRING)) AS source_ref,
        YEAR(played_at) AS played_at_year
    FROM plays
    WHERE listener_id IS NOT NULL
),
source_stats AS (
    -- Avg played_at_year per source
    SELECT source, AVG(played_at_year) AS avg_played_at_year
    FROM valid_plays
    GROUP BY source
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_played_at_year,
        ROUND(s.played_at_year - g.avg_played_at_year, 2) AS played_at_year_minus_avg
    FROM valid_plays s
    JOIN source_stats g ON g.source = s.source
    WHERE s.played_at_year > g.avg_played_at_year
),
with_parent_listener AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.is_skipped,
        s.source,
        s.source_ref,
        s.played_at_year_minus_avg,
        l.country AS listener_country,
        l.interests AS listener_interests,
        l.signup_date AS listener_signup_date,
        t.moods AS track_moods,
        t.bpm AS track_bpm
    FROM vs_group s
    JOIN listeners l ON l.id = s.listener_id AND UPPER(TRIM(l.username)) LIKE 'F%'
    JOIN tracks t ON t.id = s.track_id
)
-- Subtotals with GROUPING SETS
SELECT
    track_id,
    source,
    MIN(played_at) AS first_played_at,
    SUM(CASE WHEN listener_country = 'KR' THEN 1 ELSE 0 END) AS n_listener_country_kr
FROM with_parent_listener
GROUP BY GROUPING SETS ((track_id, source), (track_id), (source), ())
