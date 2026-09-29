WITH prepared_plays AS (
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
        DATEDIFF(DATE '2025-12-31', played_at) AS days_since_played_at,
        DAYOFWEEK(played_at) AS played_at_dayofweek,
        CASE WHEN device IN ('desktop', 'phone') THEN 'group_a' WHEN device = 'speaker' THEN 'group_b' ELSE 'other' END AS device_group
    FROM plays
    WHERE ms_played BETWEEN 112786 AND 251066
      AND is_skipped = FALSE
),
annotated AS (
    -- Derive helper columns
    SELECT *, DATE_TRUNC('DAY', played_at) AS played_at_day
    FROM prepared_plays
),
per_group AS (
    -- Summarise per group
    SELECT
        YEAR(played_at) AS played_at_year,
        source,
        track_id,
        ROUND(100 * COUNT(CASE WHEN is_skipped = FALSE THEN 1 END) / COUNT(*), 1) AS pct_is_skipped_false,
        ROUND(AVG(ms_played), 2) AS avg_ms_played,
        SUM(ms_played) AS total_ms_played,
        PERCENTILE_APPROX(played_at_dayofweek, 0.5) AS median_played_at_dayofweek
    FROM annotated
    WHERE listener_id <= 12
    GROUP BY YEAR(played_at), source, track_id
    HAVING AVG(ms_played) > 73625
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        played_at_year,
        source,
        track_id,
        pct_is_skipped_false,
        avg_ms_played,
        total_ms_played,
        median_played_at_dayofweek,
        ROUND(pct_is_skipped_false / NULLIF(SUM(pct_is_skipped_false) OVER (PARTITION BY source), 0), 4) AS pct_is_skipped_false_share_of_source,
        RANK() OVER (PARTITION BY source ORDER BY pct_is_skipped_false DESC) AS pct_is_skipped_false_rank,
        LAG(avg_ms_played, 3) OVER w_source_by_pct_is_skipped_fal_2 AS prev3_avg_ms_played
    FROM per_group
    WINDOW w_source_by_pct_is_skipped_fal_2 AS (PARTITION BY source ORDER BY pct_is_skipped_false DESC, played_at_year, source, track_id)
),
enriched_track AS (
    -- Attach track attributes
    SELECT
        s.played_at_year,
        s.source,
        s.track_id,
        s.pct_is_skipped_false,
        s.avg_ms_played,
        s.total_ms_played,
        s.median_played_at_dayofweek,
        s.pct_is_skipped_false_share_of_source,
        s.pct_is_skipped_false_rank,
        t.artist_id AS track_artist_id,
        t.duration_sec AS track_duration_sec,
        t.title AS track_title,
        a.is_verified AS artist_is_verified
    FROM windowed s
    JOIN tracks t ON t.id = s.track_id
    LEFT JOIN artists a ON a.id = t.artist_id
)
SELECT
    played_at_year,
    track_id,
    avg_ms_played,
    total_ms_played,
    median_played_at_dayofweek,
    pct_is_skipped_false_share_of_source,
    pct_is_skipped_false_rank,
    track_artist_id,
    track_duration_sec
FROM enriched_track
WHERE artist_is_verified = FALSE
ORDER BY pct_is_skipped_false_share_of_source DESC NULLS FIRST
