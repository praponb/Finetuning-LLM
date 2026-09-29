WITH recent_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM tracks
    WHERE (UPPER(TRIM(title)) LIKE 'G%' OR duration_sec > 213)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY is_explicit ORDER BY duration_sec ASC) AS duration_sec_rank,
        FIRST_VALUE(duration_sec) IGNORE NULLS OVER (PARTITION BY is_explicit ORDER BY duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY MONTH(release_date), is_explicit ORDER BY duration_sec, id) AS seq_no,
        ROUND(bpm - AVG(bpm) OVER w_is_explicit_artist_id, 2) AS bpm_vs_is_explicit_artist_id_avg
    FROM recent_tracks
    WINDOW w_is_explicit_artist_id AS (PARTITION BY is_explicit, artist_id)
),
with_windows_2 AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        duration_sec_rank,
        seq_no,
        LEAD(bpm) OVER (ORDER BY duration_sec_rank, id) AS next_bpm,
        ROUND(first_known_duration_sec / NULLIF(SUM(first_known_duration_sec) OVER (PARTITION BY duration_sec_rank, artist_id), 0), 4) AS first_known_duration_sec_share_of_duration_sec_r,
        ROUND(duration_sec_rank / NULLIF(SUM(duration_sec_rank) OVER (PARTITION BY duration_sec_rank, artist_id), 0), 4) AS duration_sec_rank_share_of_duration_sec_rank_art,
        LAST_VALUE(seq_no) OVER (ORDER BY duration_sec_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_seq_no,
        SUM(first_known_duration_sec) OVER (PARTITION BY duration_sec_rank, artist_id) AS duration_sec_rank_artist_id_sum_first_known_dura,
        ROUND(CUME_DIST() OVER w_all_by_duration_sec_rank_tie, 3) AS duration_sec_rank_cume_dist
    FROM with_windows
    WINDOW w_all_by_duration_sec_rank_tie AS (ORDER BY duration_sec_rank)
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_windows_2
    WHERE last_seq_no > (SELECT AVG(last_seq_no) FROM with_windows_2)
),
labeled AS (
    -- Add calculated columns
    SELECT *, LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug
    FROM filtered
),
without_matches AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM labeled s
    WHERE s.artist_id IN (
            SELECT t.artist_id
            FROM tracks t
            WHERE UPPER(TRIM(t.title)) LIKE 'C%'
              AND SIZE(t.moods) >= 2
        )
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    is_explicit,
    bpm,
    duration_sec_rank_share_of_duration_sec_rank_art,
    last_seq_no,
    duration_sec_rank_cume_dist,
    title_slug,
    COALESCE(next_bpm, 0) AS next_bpm_filled
FROM without_matches
