WITH valid_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug,
        LEAST(bpm, duration_sec) AS least_bpm_duration_sec
    FROM tracks
    WHERE SIZE(moods) >= 1
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(bpm) OVER (PARTITION BY artist_id ORDER BY bpm, id) AS prev_bpm,
        LEAD(bpm, 3) OVER w_all_by_duration_sec AS next3_bpm,
        LAST_VALUE(duration_sec) OVER (ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_duration_sec,
        ROUND(AVG(least_bpm_duration_sec) OVER (PARTITION BY artist_id), 2) AS artist_id_avg_least_bpm_duration_sec,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', release_date) ORDER BY release_date DESC NULLS FIRST, id), 3) AS release_date_percent_rank,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', release_date) ORDER BY least_bpm_duration_sec NULLS LAST, id) AS least_bpm_duration_sec_rank,
        FIRST_VALUE(release_date) IGNORE NULLS OVER (ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_release_date,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', release_date) ORDER BY release_date DESC NULLS FIRST), 3) AS release_date_percent_rank_2,
        bpm - LAG(bpm) OVER (PARTITION BY artist_id ORDER BY bpm, id) AS bpm_change,
        FIRST_VALUE(duration_sec) IGNORE NULLS OVER (PARTITION BY artist_id ORDER BY bpm ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec,
        DENSE_RANK() OVER (ORDER BY duration_sec DESC, id) AS duration_sec_dense_rank
    FROM valid_tracks
    WINDOW w_all_by_duration_sec AS (ORDER BY duration_sec DESC, id), w_artist_id AS (PARTITION BY artist_id)
),
first_rows AS (
    -- First row per group
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        title_slug,
        least_bpm_duration_sec,
        prev_bpm,
        next3_bpm,
        last_duration_sec,
        artist_id_avg_least_bpm_duration_sec,
        release_date_percent_rank,
        least_bpm_duration_sec_rank,
        first_known_release_date,
        release_date_percent_rank_2,
        bpm_change,
        first_known_duration_sec
    FROM metrics
    WHERE duration_sec_dense_rank = 1
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(prev_bpm) OVER (PARTITION BY artist_id, least_bpm_duration_sec_rank), 2) AS artist_id_least_bpm_duration_sec_rank_avg_prev_b,
        FIRST_VALUE(prev_bpm) IGNORE NULLS OVER (PARTITION BY artist_id, least_bpm_duration_sec_rank ORDER BY prev_bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_prev_bpm,
        LAG(next3_bpm) OVER (PARTITION BY artist_id, least_bpm_duration_sec_rank ORDER BY prev_bpm, id) AS prev_next3_bpm,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_id, least_bpm_duration_sec_rank ORDER BY prev_bpm), 3) AS prev_bpm_percent_rank,
        DENSE_RANK() OVER (PARTITION BY least_bpm_duration_sec_rank, artist_id ORDER BY release_date_percent_rank) AS release_date_percent_rank_dense_rank
    FROM first_rows
)
SELECT
    id,
    release_date,
    bpm,
    title_slug,
    least_bpm_duration_sec,
    prev_bpm,
    last_duration_sec,
    release_date_percent_rank,
    least_bpm_duration_sec_rank,
    release_date_percent_rank_2,
    bpm_change,
    first_known_duration_sec,
    artist_id_least_bpm_duration_sec_rank_avg_prev_b,
    prev_next3_bpm,
    prev_bpm_percent_rank,
    release_date_percent_rank_dense_rank,
    DATE_TRUNC('QUARTER', release_date) AS release_date_quarter
FROM sequenced
WHERE bpm <= 142
ORDER BY prev_next3_bpm DESC, release_date_percent_rank DESC, id
LIMIT 5
