WITH scoped_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm
    FROM tracks
    WHERE SIZE(moods) >= 1
      AND artist_id IS NOT NULL
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        title,
        release_date,
        is_explicit,
        bpm,
        COUNT(*) OVER (ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_id), 2) AS artist_id_avg_bpm,
        LAST_VALUE(bpm) IGNORE NULLS OVER (ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_bpm,
        FIRST_VALUE(release_date) IGNORE NULLS OVER (ORDER BY release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_release_date,
        SUM(bpm) OVER w_artist_id AS artist_id_sum_bpm
    FROM scoped_tracks
    WINDOW w_artist_id AS (PARTITION BY artist_id)
)
SELECT
    id,
    title,
    release_date,
    is_explicit,
    bpm,
    last_known_bpm,
    first_known_release_date,
    artist_id_sum_bpm,
    DATE_TRUNC('WEEK', first_known_release_date) AS first_known_release_date_week,
    CASE WHEN running_count >= 13 THEN 'A' WHEN running_count >= 10 THEN 'B' ELSE 'C' END AS running_count_tier
FROM ranked
WHERE artist_id_sum_bpm < 1191
ORDER BY first_known_release_date_week DESC NULLS FIRST, id
