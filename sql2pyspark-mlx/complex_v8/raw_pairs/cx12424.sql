WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        bpm,
        moods,
        SUBSTRING(TRIM(title), 1, 4) AS title_prefix,
        FLOOR(bpm / 25) * 25 AS bpm_bucket
    FROM tracks
    WHERE is_explicit = TRUE
),
with_activity AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM prepared s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND NOT t.is_explicit
              AND t.release_date BETWEEN DATE '2024-08-26' AND DATE '2025-04-02'
        )
),
qualified AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM with_activity s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
        )
      AND s.artist_id <= 9
),
artist_id_stats AS (
    -- Avg bpm per artist_id
    SELECT artist_id, AVG(bpm) AS avg_bpm
    FROM qualified
    GROUP BY artist_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_bpm, ROUND(s.bpm - g.avg_bpm, 2) AS bpm_minus_avg
    FROM qualified s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.bpm > g.avg_bpm
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(avg_bpm - AVG(avg_bpm) OVER (PARTITION BY is_explicit), 2) AS avg_bpm_vs_is_explicit_avg,
        NTH_VALUE(bpm_minus_avg, 2) OVER (PARTITION BY is_explicit ORDER BY bpm_bucket NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm_minus_avg,
        FIRST_VALUE(bpm) OVER (PARTITION BY is_explicit ORDER BY bpm_bucket NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_bpm,
        SIZE(COLLECT_SET(artist_id) OVER w_is_explicit) AS distinct_artist_id_in_is_explicit,
        COUNT(*) OVER w_is_explicit AS rows_in_is_explicit,
        NTILE(3) OVER (PARTITION BY is_explicit, artist_id ORDER BY bpm_minus_avg DESC, id) AS bpm_minus_avg_tercile
    FROM compared
    WINDOW w_is_explicit AS (PARTITION BY is_explicit)
)
SELECT
    id,
    artist_id,
    title,
    is_explicit,
    title_prefix,
    bpm_bucket,
    bpm_minus_avg,
    avg_bpm_vs_is_explicit_avg,
    second_bpm_minus_avg,
    first_bpm,
    distinct_artist_id_in_is_explicit,
    rows_in_is_explicit,
    bpm_minus_avg_tercile,
    second_bpm_minus_avg - bpm_bucket AS second_bpm_minus_avg_minus_bpm_bucket
FROM sequenced
ORDER BY title_prefix DESC, bpm_minus_avg NULLS LAST
