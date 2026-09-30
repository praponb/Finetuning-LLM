WITH valid AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        bpm,
        moods,
        CASE WHEN duration_sec >= 352 THEN 'HIGH' WHEN duration_sec >= 184 THEN 'MEDIUM' ELSE 'LOW' END AS duration_sec_tier,
        CASE WHEN bpm >= 147 THEN 'large' WHEN bpm >= 96 THEN 'medium' ELSE 'small' END AS bpm_tier
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'chill')
      AND UPPER(TRIM(title)) LIKE 'C%'
),
summarized_rows AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT SUM(t.bpm) FROM tracks t WHERE t.artist_id = s.artist_id) AS total_track_bpm
    FROM valid s
)
-- Subtotals with GROUPING SETS
SELECT
    duration_sec_tier,
    artist_id,
    bpm_tier,
    SUM(CASE WHEN NOT is_explicit THEN bpm ELSE 0 END) AS bpm_not_is_explicit,
    ROUND(100 * COUNT(CASE WHEN NOT is_explicit THEN 1 END) / COUNT(*), 1) AS pct_not_is_explicit,
    DATEDIFF(MAX(release_date), MIN(release_date)) AS release_date_span_days
FROM summarized_rows
GROUP BY GROUPING SETS ((duration_sec_tier, artist_id, bpm_tier), (duration_sec_tier), ())
ORDER BY duration_sec_tier NULLS LAST, artist_id NULLS LAST, bpm_tier NULLS LAST
