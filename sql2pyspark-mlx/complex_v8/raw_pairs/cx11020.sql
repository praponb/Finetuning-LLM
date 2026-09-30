WITH cleaned_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown,
        played_at + INTERVAL 15 MINUTES AS played_at_plus_15m
    FROM plays
    WHERE listener_id <= 11
),
joined AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.source,
        s.source_slug,
        s.source_or_unknown,
        t.moods AS track_moods,
        a.debut_year AS artist_debut_year,
        a.genre AS artist_genre
    FROM cleaned_plays s
    LEFT JOIN tracks t ON t.id = s.track_id
    LEFT JOIN artists a ON a.id = t.artist_id
),
enriched AS (
    -- Add calculated columns
    SELECT *, DATE_FORMAT(played_at, 'MMM yyyy') AS played_at_label
    FROM joined
    WHERE artist_debut_year <= 2016
),
listener_id_stats AS (
    -- Median ms_played per listener_id
    SELECT listener_id, PERCENTILE_APPROX(ms_played, 0.5) AS median_ms_played
    FROM enriched
    GROUP BY listener_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_ms_played,
        ROUND(s.ms_played - g.median_ms_played, 2) AS ms_played_minus_median
    FROM enriched s
    JOIN listener_id_stats g ON g.listener_id = s.listener_id
    WHERE s.ms_played >= g.median_ms_played
),
flattened AS (
    -- One row per track_mood
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.source,
        s.source_slug,
        s.source_or_unknown,
        s.artist_debut_year,
        s.artist_genre,
        s.played_at_label,
        s.median_ms_played,
        s.ms_played_minus_median,
        t.track_mood
    FROM compared s
    LATERAL VIEW OUTER EXPLODE(s.track_moods) t AS track_mood
),
joined_2 AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.source,
        s.source_slug,
        s.artist_debut_year,
        s.played_at_label,
        s.median_ms_played,
        s.ms_played_minus_median,
        s.track_mood,
        t.is_explicit AS track_is_explicit,
        t.bpm AS track_bpm,
        l.username AS listener_username
    FROM flattened s
    JOIN tracks t ON t.id = s.track_id
    LEFT JOIN listeners l ON l.id = s.listener_id
)
SELECT
    id,
    track_id,
    source,
    source_slug,
    artist_debut_year,
    played_at_label,
    median_ms_played,
    track_mood,
    track_is_explicit,
    track_bpm,
    listener_username,
    ROUND(ms_played_minus_median * 0.25, 2) AS ms_played_minus_median_adj,
    CASE WHEN source IN ('search', 'playlist') THEN 'group_a' WHEN source = 'radio' THEN 'group_b' ELSE 'other' END AS source_group
FROM joined_2
ORDER BY played_at_label DESC, source_slug
