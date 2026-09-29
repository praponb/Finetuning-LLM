WITH scoped AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        COALESCE(duration_sec, 0) AS duration_sec_filled
    FROM tracks
    WHERE release_date BETWEEN DATE '2024-04-29' AND DATE '2025-08-02'
      AND bpm <= 142
),
artist_id_stats AS (
    -- Avg bpm per artist_id
    SELECT artist_id, AVG(bpm) AS avg_bpm
    FROM scoped
    GROUP BY artist_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_bpm, ROUND(s.bpm - g.avg_bpm, 2) AS bpm_minus_avg
    FROM scoped s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.bpm > g.avg_bpm
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        duration_sec_filled,
        avg_bpm,
        ARRAY_CONTAINS(moods, 'sad') AS has_sad,
        ROUND(avg_bpm / NULLIF(bpm, 0), 3) AS avg_bpm_per_bpm
    FROM above_average
    WHERE bpm_minus_avg < 54.0
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        duration_sec_filled,
        has_sad,
        avg_bpm_per_bpm,
        COUNT(*) OVER w_is_explicit AS rows_in_is_explicit,
        MIN(bpm) OVER (ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_bpm_so_far,
        MAX(bpm) OVER w_is_explicit AS is_explicit_max_bpm,
        MIN(avg_bpm_per_bpm) OVER (PARTITION BY is_explicit) AS is_explicit_min_avg_bpm_per_bpm,
        LAG(duration_sec) OVER (ORDER BY bpm DESC, id) AS prev_duration_sec
    FROM enriched
    WINDOW w_is_explicit AS (PARTITION BY is_explicit)
)
SELECT
    id,
    artist_id,
    duration_sec,
    release_date,
    is_explicit,
    duration_sec_filled,
    has_sad,
    rows_in_is_explicit,
    min_bpm_so_far,
    is_explicit_max_bpm,
    prev_duration_sec,
    min_bpm_so_far - avg_bpm_per_bpm AS min_bpm_so_far_minus_avg_bpm_per_bpm,
    is_explicit_max_bpm + rows_in_is_explicit AS is_explicit_max_bpm_plus_rows_in_is_explicit
FROM ranked
ORDER BY min_bpm_so_far NULLS LAST, id
LIMIT 20
