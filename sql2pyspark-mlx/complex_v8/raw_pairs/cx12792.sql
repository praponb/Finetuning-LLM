WITH recent_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods
    FROM tracks
    WHERE artist_id IS NOT NULL
      AND NOT ARRAY_CONTAINS(moods, 'chill')
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(duration_sec) OVER (ORDER BY release_date DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_2,
        SUM(duration_sec) OVER (PARTITION BY is_explicit) AS is_explicit_sum_duration_sec,
        COUNT(*) OVER (PARTITION BY is_explicit) AS rows_in_is_explicit,
        RANK() OVER (PARTITION BY is_explicit ORDER BY duration_sec DESC, id) AS duration_sec_rank,
        DENSE_RANK() OVER w_is_explicit_by_duration_sec_2 AS duration_sec_dense_rank,
        NTILE(3) OVER w_all_by_duration_sec AS duration_sec_tercile,
        LAST_VALUE(duration_sec) IGNORE NULLS OVER (PARTITION BY artist_id ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec,
        ROUND(duration_sec - AVG(duration_sec) OVER w_artist_id_is_explicit, 2) AS duration_sec_vs_artist_id_is_explicit_avg
    FROM recent_tracks
    WINDOW w_is_explicit_by_duration_sec_2 AS (PARTITION BY is_explicit ORDER BY duration_sec), w_all_by_duration_sec AS (ORDER BY duration_sec, id), w_artist_id_is_explicit AS (PARTITION BY artist_id, is_explicit)
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        FLOOR(duration_sec_rank / 2) * 2 AS duration_sec_rank_bucket,
        DATE_ADD(release_date, 30) AS release_date_plus_30d
    FROM sequenced
    WHERE rows_in_is_explicit > 3
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        title,
        release_date,
        duration_sec_avg_2,
        is_explicit_sum_duration_sec,
        duration_sec_rank,
        duration_sec_tercile,
        last_known_duration_sec,
        duration_sec_vs_artist_id_is_explicit_avg,
        duration_sec_rank_bucket,
        release_date_plus_30d,
        SUM(duration_sec_avg_2) OVER (PARTITION BY duration_sec_rank, MONTH(release_date) ORDER BY duration_sec_rank DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS duration_sec_avg_2_sum_2_prior,
        ROUND(PERCENT_RANK() OVER (ORDER BY duration_sec DESC NULLS FIRST), 3) AS duration_sec_percent_rank,
        duration_sec_tercile - LAG(duration_sec_tercile) OVER (ORDER BY duration_sec DESC NULLS FIRST, id) AS duration_sec_tercile_change,
        LAG(duration_sec) OVER w_all_by_duration_sec_rank AS prev_duration_sec,
        COUNT(*) OVER (PARTITION BY artist_id, duration_sec_dense_rank) AS rows_in_artist_id_duration_sec_dense_rank,
        ROUND(duration_sec_tercile - AVG(duration_sec_tercile) OVER w_artist_id_duration_sec_dense, 2) AS duration_sec_tercile_vs_artist_id_duration_sec_d,
        LAST_VALUE(duration_sec_rank_bucket) IGNORE NULLS OVER (PARTITION BY duration_sec_rank, MONTH(release_date) ORDER BY duration_sec_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec_rank_bucket,
        NTH_VALUE(release_date_plus_30d, 2) OVER (PARTITION BY duration_sec_rank, MONTH(release_date) ORDER BY duration_sec_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date_plus_30d
    FROM derived
    WINDOW w_all_by_duration_sec_rank AS (ORDER BY duration_sec_rank DESC, id), w_artist_id_duration_sec_dense AS (PARTITION BY artist_id, duration_sec_dense_rank)
)
SELECT
    id,
    artist_id,
    release_date,
    duration_sec_avg_2,
    is_explicit_sum_duration_sec,
    duration_sec_rank,
    duration_sec_tercile,
    last_known_duration_sec,
    duration_sec_vs_artist_id_is_explicit_avg,
    duration_sec_rank_bucket,
    release_date_plus_30d,
    duration_sec_avg_2_sum_2_prior,
    duration_sec_percent_rank,
    prev_duration_sec,
    rows_in_artist_id_duration_sec_dense_rank,
    second_release_date_plus_30d,
    INITCAP(TRIM(title)) AS title_clean
FROM with_windows
WHERE duration_sec_tercile_vs_artist_id_duration_sec_d > -3.0
ORDER BY prev_duration_sec, id
LIMIT 10
