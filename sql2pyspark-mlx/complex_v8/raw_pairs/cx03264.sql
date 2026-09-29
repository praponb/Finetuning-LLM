WITH valid AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        ROUND(duration_sec / NULLIF(bpm, 0), 3) AS duration_sec_per_bpm,
        COALESCE(bpm, 0) AS bpm_filled
    FROM tracks
),
with_activity AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
        )
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(bpm_filled - AVG(bpm_filled) OVER (PARTITION BY is_explicit, artist_id), 2) AS bpm_filled_vs_is_explicit_artist_id_avg,
        ROUND(CUME_DIST() OVER (ORDER BY duration_sec), 3) AS duration_sec_cume_dist,
        RANK() OVER (ORDER BY duration_sec) AS duration_sec_rank,
        ROW_NUMBER() OVER w_all_by_duration_sec AS seq_no,
        FIRST_VALUE(duration_sec_per_bpm) IGNORE NULLS OVER (ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec_per_bpm
    FROM with_activity
    WINDOW w_all_by_duration_sec AS (ORDER BY duration_sec, id), w_all_by_duration_sec_ties AS (ORDER BY duration_sec)
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    is_explicit,
    duration_sec_per_bpm,
    bpm_filled,
    bpm_filled_vs_is_explicit_artist_id_avg,
    duration_sec_cume_dist,
    seq_no,
    first_known_duration_sec_per_bpm,
    ROUND(seq_no / NULLIF(duration_sec, 0), 3) AS seq_no_per_duration_sec,
    GREATEST(duration_sec_per_bpm, duration_sec_cume_dist) AS greatest_duration_sec_per_bpm_duration_sec_cume_
FROM ranked
WHERE seq_no BETWEEN 1 AND 9
ORDER BY first_known_duration_sec_per_bpm DESC NULLS FIRST, title DESC, id
LIMIT 10
