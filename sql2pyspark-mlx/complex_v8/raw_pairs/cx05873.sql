WITH base_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix,
        CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag,
        LENGTH(TRIM(title)) AS title_len
    FROM tracks
    WHERE release_date >= DATE '2024-11-14'
      AND bpm <= 127
),
with_activity AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM base_tracks s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND SIZE(t.moods) >= 2
        )
),
screened AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM with_activity s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.source NOT IN ('playlist', 'search')
              AND p.device IN ('speaker', 'car')
        )
      AND s.title_len <= 12
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(is_explicit_flag) OVER (PARTITION BY artist_id, title_len), 2) AS artist_id_title_len_avg_is_explicit_flag,
        LAST_VALUE(duration_sec) OVER (ORDER BY is_explicit_flag DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_duration_sec,
        ROUND(AVG(title_len) OVER (ORDER BY is_explicit_flag DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS title_len_avg_1_prior,
        FIRST_VALUE(release_date) OVER (ORDER BY is_explicit_flag DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_release_date,
        NTILE(10) OVER (ORDER BY is_explicit_flag DESC, id) AS is_explicit_flag_decile
    FROM screened
)
-- Subtotals with ROLLUP
SELECT
    title_len,
    is_explicit,
    artist_id,
    GROUPING_ID() AS grouping_level,
    MAX(title_len_avg_1_prior) AS max_title_len_avg_1_prior,
    MIN(is_explicit_flag_decile) AS min_is_explicit_flag_decile
FROM ranked
GROUP BY ROLLUP(title_len, is_explicit, artist_id)
ORDER BY title_len NULLS LAST, is_explicit NULLS LAST, artist_id NULLS LAST
