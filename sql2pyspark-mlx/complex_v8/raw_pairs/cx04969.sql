WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM tracks
    WHERE bpm <= 142
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        duration_sec - LAG(duration_sec) OVER (PARTITION BY artist_id ORDER BY duration_sec ASC, id) AS duration_sec_change,
        ROUND(CUME_DIST() OVER (PARTITION BY is_explicit ORDER BY duration_sec DESC NULLS FIRST, id), 3) AS duration_sec_cume_dist,
        COUNT(*) OVER (PARTITION BY artist_id ORDER BY duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LAG(release_date) OVER (PARTITION BY is_explicit ORDER BY duration_sec, id) AS prev_release_date,
        LAST_VALUE(bpm) OVER (PARTITION BY is_explicit, artist_id ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_bpm,
        LAST_VALUE(bpm) IGNORE NULLS OVER (PARTITION BY is_explicit ORDER BY duration_sec DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_bpm,
        LEAD(duration_sec, 3, 0) OVER (PARTITION BY is_explicit, artist_id ORDER BY bpm DESC, id) AS next3_duration_sec,
        ROUND(duration_sec - AVG(duration_sec) OVER w_artist_id_is_explicit, 2) AS duration_sec_vs_artist_id_is_explicit_avg
    FROM recent
    WINDOW w_artist_id_is_explicit AS (PARTITION BY artist_id, is_explicit)
),
enriched AS (
    -- Add calculated columns
    SELECT *, LENGTH(TRIM(title)) AS title_len
    FROM sequenced
)
SELECT
    id,
    title,
    duration_sec,
    is_explicit,
    bpm,
    moods,
    duration_sec_change,
    running_count,
    prev_release_date,
    last_bpm,
    last_known_bpm,
    next3_duration_sec,
    duration_sec_vs_artist_id_is_explicit_avg,
    title_len
FROM enriched
ORDER BY bpm DESC, id
LIMIT 5
