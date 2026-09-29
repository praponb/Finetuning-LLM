WITH valid AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        moods,
        ABS(duration_sec - 352) AS duration_sec_distance,
        LENGTH(TRIM(title)) AS title_len
    FROM tracks
),
enriched_artist AS (
    -- Enrich with dimension data
    SELECT s.*, a.debut_year AS artist_debut_year, a.artist_name
    FROM valid s
    LEFT JOIN artists a ON a.id = s.artist_id AND a.is_verified = FALSE
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        is_explicit,
        moods,
        duration_sec_distance,
        artist_debut_year,
        FIRST_VALUE(artist_debut_year) IGNORE NULLS OVER (PARTITION BY title_len, is_explicit ORDER BY duration_sec_distance NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_artist_debut_year,
        LAG(artist_debut_year) OVER w_all_by_duration_sec_distance AS prev_artist_debut_year,
        ROUND(AVG(duration_sec_distance) OVER (PARTITION BY YEAR(release_date), artist_id), 2) AS release_date_year_artist_id_avg_duration_sec_dis,
        NTH_VALUE(artist_debut_year, 2) OVER (ORDER BY duration_sec_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_artist_debut_year,
        ROUND(AVG(title_len) OVER (PARTITION BY YEAR(release_date), artist_id), 2) AS release_date_year_artist_id_avg_title_len,
        LEAD(duration_sec_distance, 3) OVER w_all_by_duration_sec_distance AS next3_duration_sec_distance,
        MAX(artist_debut_year) OVER w_release_date_year_artist_id AS release_date_year_artist_id_max_artist_debut_yea,
        MAX(title_len) OVER (ORDER BY title_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_title_len_so_far,
        SUM(duration_sec_distance) OVER w_release_date_year_artist_id AS release_date_year_artist_id_sum_duration_sec_dis,
        LEAD(title_len, 1, 0) OVER (ORDER BY title_len, id) AS next_title_len,
        FIRST_VALUE(duration_sec_distance) OVER (PARTITION BY title_len, is_explicit ORDER BY duration_sec_distance NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_duration_sec_distance,
        NTILE(5) OVER w_title_len_is_explicit_by_dur_3 AS duration_sec_distance_quintile
    FROM enriched_artist
    WINDOW w_all_by_duration_sec_distance AS (ORDER BY duration_sec_distance, id), w_release_date_year_artist_id AS (PARTITION BY YEAR(release_date), artist_id), w_title_len_is_explicit_by_dur_3 AS (PARTITION BY title_len, is_explicit ORDER BY duration_sec_distance NULLS LAST, id)
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        moods,
        duration_sec_distance,
        first_known_artist_debut_year,
        release_date_year_artist_id_avg_duration_sec_dis,
        second_artist_debut_year,
        release_date_year_artist_id_avg_title_len,
        max_title_len_so_far,
        release_date_year_artist_id_sum_duration_sec_dis,
        first_duration_sec_distance,
        duration_sec_distance_quintile,
        FLOOR(artist_debut_year / 5) * 5 AS artist_debut_year_bucket
    FROM metrics
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(first_duration_sec_distance) OVER (PARTITION BY duration_sec_distance_quintile ORDER BY release_date_year_artist_id_avg_duration_sec_dis DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_first_duration_sec_distance_so_far,
        SUM(duration_sec_distance) OVER (ORDER BY first_duration_sec_distance, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS duration_sec_distance_sum_4,
        ROW_NUMBER() OVER (ORDER BY first_duration_sec_distance, id) AS seq_no,
        LEAD(second_artist_debut_year, 3, 0) OVER (ORDER BY first_duration_sec_distance, id) AS next3_second_artist_debut_year,
        LAG(max_title_len_so_far) OVER w_duration_sec_distance_quinti_2 AS prev_max_title_len_so_far,
        LAST_VALUE(first_duration_sec_distance) IGNORE NULLS OVER (PARTITION BY duration_sec_distance_quintile ORDER BY release_date_year_artist_id_avg_duration_sec_dis DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_first_duration_sec_distance,
        MIN(second_artist_debut_year) OVER (PARTITION BY duration_sec_distance_quintile) AS duration_sec_distance_quintile_min_second_artist,
        ROUND(AVG(first_duration_sec_distance) OVER (PARTITION BY duration_sec_distance_quintile), 2) AS duration_sec_distance_quintile_avg_first_duratio
    FROM enriched
    WINDOW w_duration_sec_distance_quinti_2 AS (PARTITION BY duration_sec_distance_quintile ORDER BY release_date_year_artist_id_avg_duration_sec_dis DESC, id)
),
rollup_base AS (
    -- Group-level totals
    SELECT
        duration_sec_distance_quintile,
        SUM(artist_debut_year_bucket) AS total_artist_debut_year_bucket,
        MAX(artist_debut_year_bucket) AS max_artist_debut_year_bucket,
        COUNT(DISTINCT id) AS distinct_ids
    FROM windowed
    GROUP BY 1
),
windowed_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY max_artist_debut_year_bucket DESC), 3) AS max_artist_debut_year_bucket_cume_dist,
        LEAD(distinct_ids, 3) OVER (ORDER BY max_artist_debut_year_bucket DESC, duration_sec_distance_quintile) AS next3_distinct_ids,
        ROUND(CUME_DIST() OVER (ORDER BY max_artist_debut_year_bucket DESC, duration_sec_distance_quintile), 3) AS max_artist_debut_year_bucket_cume_dist_2
    FROM rollup_base
)
-- Final projection
SELECT
    duration_sec_distance_quintile,
    total_artist_debut_year_bucket,
    max_artist_debut_year_bucket,
    distinct_ids,
    max_artist_debut_year_bucket_cume_dist,
    next3_distinct_ids,
    max_artist_debut_year_bucket_cume_dist_2,
    distinct_ids + next3_distinct_ids AS distinct_ids_plus_next3_distinct_ids
FROM windowed_2
ORDER BY next3_distinct_ids DESC NULLS FIRST, max_artist_debut_year_bucket_cume_dist_2 NULLS LAST
