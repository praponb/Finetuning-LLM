WITH filtered_tracks AS (
    -- Scope the tracks rows
    SELECT *
    FROM tracks
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY MONTH(release_date), is_explicit), 0), 4) AS bpm_share_of_release_date_month_is_explicit,
        LEAD(release_date) OVER (PARTITION BY MONTH(release_date), is_explicit ORDER BY release_date DESC, id) AS next_release_date
    FROM filtered_tracks
),
artist_id_stats AS (
    -- Avg bpm per artist_id
    SELECT artist_id, AVG(bpm) AS avg_bpm
    FROM with_windows
    GROUP BY artist_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_bpm, ROUND(s.bpm - g.avg_bpm, 2) AS bpm_minus_avg
    FROM with_windows s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.bpm > g.avg_bpm
),
flattened AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.bpm_share_of_release_date_month_is_explicit,
        s.next_release_date,
        s.avg_bpm,
        s.bpm_minus_avg,
        t.mood
    FROM above_average s
    LATERAL VIEW EXPLODE(s.moods) t AS mood
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(avg_bpm) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), is_explicit ORDER BY avg_bpm, id, mood ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS avg_bpm_max_6_prior,
        ROUND(AVG(bpm_minus_avg) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), is_explicit ORDER BY avg_bpm, id, mood ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS bpm_minus_avg_avg_2
    FROM flattened
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, mood, metric_name, amount
FROM (
    SELECT id, mood, bpm_minus_avg, avg_bpm_max_6_prior, bpm_minus_avg_avg_2
    FROM ranked
) src
UNPIVOT INCLUDE NULLS (
    amount FOR metric_name IN (bpm_minus_avg AS bpm_minus_avg_v, avg_bpm_max_6_prior AS m_avg_bpm_max_6_prior, bpm_minus_avg_avg_2 AS m_bpm_minus_avg_avg_2)
) unp
ORDER BY id, mood, metric_name
