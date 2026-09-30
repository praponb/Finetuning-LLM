WITH scoped_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        CASE WHEN ms_played >= 239955 THEN 'A' WHEN ms_played >= 73177 THEN 'B' ELSE 'C' END AS ms_played_tier,
        SUBSTRING(TRIM(device), 1, 3) AS device_prefix,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug
    FROM plays
    WHERE track_id >= 3
      AND device NOT IN ('tv', 'desktop')
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        device_prefix,
        LAG(ms_played) OVER w_listener_id_by_ms_played AS prev_ms_played,
        ROUND(AVG(ms_played) OVER (PARTITION BY listener_id ORDER BY ms_played DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_2_prior,
        MIN(ms_played) OVER (PARTITION BY MONTH(played_at)) AS played_at_month_min_ms_played,
        SUM(ms_played) OVER (PARTITION BY MONTH(played_at)) AS played_at_month_sum_ms_played
    FROM scoped_plays
    WINDOW w_listener_id_by_ms_played AS (PARTITION BY listener_id ORDER BY ms_played DESC, id)
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.is_skipped,
        s.source,
        s.device_prefix,
        s.ms_played_avg_2_prior,
        s.played_at_month_sum_ms_played,
        t.moods AS track_moods,
        a.genre AS artist_genre,
        a.country AS artist_country
    FROM with_windows s
    JOIN tracks t ON t.id = s.track_id
    LEFT JOIN artists a ON a.id = t.artist_id
)
-- Final projection
SELECT
    id,
    played_at,
    ms_played,
    device_prefix,
    ms_played_avg_2_prior,
    played_at_month_sum_ms_played,
    track_moods,
    artist_country,
    TO_DATE(played_at) AS played_at_date,
    ROUND(ms_played / NULLIF(ms_played_avg_2_prior, 0), 3) AS ms_played_per_ms_played_avg_2_prior
FROM combined_details
ORDER BY ms_played NULLS LAST, played_at_month_sum_ms_played DESC
