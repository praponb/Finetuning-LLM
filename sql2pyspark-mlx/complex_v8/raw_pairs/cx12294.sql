WITH base_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        ARRAY_JOIN(ARRAY_SORT(moods), ',') AS moods_text,
        DATEDIFF(DATE '2026-01-01', release_date) AS days_since_release_date
    FROM tracks
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        ROUND(CUME_DIST() OVER (ORDER BY release_date DESC NULLS FIRST), 3) AS release_date_cume_dist,
        SUM(days_since_release_date) OVER (ORDER BY days_since_release_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS days_since_release_date_sum_7,
        ROW_NUMBER() OVER w_artist_id_by_duration_sec AS duration_sec_seq,
        days_since_release_date - LAG(days_since_release_date) OVER w_all_by_release_date AS days_since_release_date_change,
        NTH_VALUE(days_since_release_date, 2) OVER (PARTITION BY artist_id ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_days_since_release_date,
        MAX(days_since_release_date) OVER (PARTITION BY artist_id) AS artist_id_max_days_since_release_date,
        SUM(days_since_release_date) OVER w_artist_id AS artist_id_sum_days_since_release_date,
        SUM(duration_sec) OVER (PARTITION BY is_explicit) AS is_explicit_sum_duration_sec,
        FIRST_VALUE(duration_sec) OVER (PARTITION BY artist_id ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_duration_sec,
        FIRST_VALUE(days_since_release_date) IGNORE NULLS OVER (PARTITION BY is_explicit, artist_id ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_days_since_release_date,
        ROUND(AVG(days_since_release_date) OVER (PARTITION BY is_explicit, artist_id ORDER BY duration_sec DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS days_since_release_date_avg_2_prior
    FROM base_tracks
    WINDOW w_artist_id_by_duration_sec AS (PARTITION BY artist_id ORDER BY duration_sec, id), w_all_by_release_date AS (ORDER BY release_date DESC NULLS FIRST, id), w_artist_id AS (PARTITION BY artist_id)
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        artist_id,
        COUNT(*) AS cnt,
        MIN(second_days_since_release_date) AS min_second_days_since_release_date,
        MIN(first_known_days_since_release_date) AS min_first_known_days_since_release_date,
        MIN(days_since_release_date_avg_2_prior) AS min_days_since_release_date_avg_2_prior,
        MAX(first_duration_sec) AS max_first_duration_sec,
        COUNT(DISTINCT artist_id) AS distinct_artist_ids
    FROM with_windows
    GROUP BY 1
)
SELECT
    artist_id,
    cnt,
    min_second_days_since_release_date,
    max_first_duration_sec,
    distinct_artist_ids,
    max_first_duration_sec - cnt AS max_first_duration_sec_minus_cnt,
    LEAST(max_first_duration_sec, distinct_artist_ids) AS least_max_first_duration_sec_distinct_artist_ids
FROM totals
ORDER BY cnt DESC, distinct_artist_ids DESC, artist_id
LIMIT 5
