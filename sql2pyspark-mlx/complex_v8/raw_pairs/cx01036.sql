WITH filtered AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        DATEDIFF(DATE '2025-06-30', release_date) AS days_since_release_date
    FROM tracks
    WHERE bpm > 85
),
latest_per_key AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        days_since_release_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY bpm DESC, id DESC) AS row_num
        FROM filtered
    ) ranked
    WHERE row_num = 1
),
artist_id_stats AS (
    -- Avg bpm per artist_id
    SELECT artist_id, AVG(bpm) AS avg_bpm
    FROM latest_per_key
    GROUP BY artist_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_bpm, ROUND(s.bpm - g.avg_bpm, 2) AS bpm_minus_avg
    FROM latest_per_key s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.bpm > g.avg_bpm
)
-- Subtotals with ROLLUP
SELECT
    is_explicit,
    artist_id,
    GROUPING_ID() AS grouping_level,
    COUNT(DISTINCT artist_id) AS distinct_artist_ids,
    ROUND(100 * COUNT(CASE WHEN NOT is_explicit THEN 1 END) / COUNT(*), 1) AS pct_not_is_explicit,
    COUNT(*) AS n_rows
FROM compared
GROUP BY ROLLUP(is_explicit, artist_id)
ORDER BY is_explicit NULLS LAST, artist_id NULLS LAST
