WITH valid_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        SUBSTRING(TRIM(title), 1, 3) AS title_prefix,
        CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag
    FROM tracks
    WHERE bpm BETWEEN 76 AND 156
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(release_date, 2) OVER (PARTITION BY is_explicit ORDER BY is_explicit_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date,
        ROW_NUMBER() OVER (PARTITION BY is_explicit ORDER BY is_explicit_flag, id) AS rn,
        MIN(duration_sec) OVER (PARTITION BY is_explicit ORDER BY is_explicit_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_duration_sec_so_far,
        ROW_NUMBER() OVER w_is_explicit_flag_is_explicit AS rn_2,
        NTH_VALUE(is_explicit_flag, 2) OVER (PARTITION BY is_explicit ORDER BY is_explicit_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_is_explicit_flag,
        LEAD(duration_sec) OVER w_is_explicit_by_is_explicit_f_2 AS next_duration_sec,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_explicit ORDER BY duration_sec DESC NULLS FIRST, id), 3) AS duration_sec_percent_rank,
        LAG(duration_sec, 2, 0) OVER (PARTITION BY is_explicit ORDER BY duration_sec DESC NULLS FIRST, id) AS prev2_duration_sec,
        SUM(is_explicit_flag) OVER (PARTITION BY YEAR(release_date)) AS release_date_year_sum_is_explicit_flag,
        NTILE(3) OVER (PARTITION BY is_explicit ORDER BY is_explicit_flag, id) AS is_explicit_flag_tercile,
        ROUND(is_explicit_flag - AVG(is_explicit_flag) OVER (PARTITION BY YEAR(release_date)), 2) AS is_explicit_flag_vs_release_date_year_avg,
        FIRST_VALUE(is_explicit_flag) OVER (PARTITION BY is_explicit ORDER BY duration_sec DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_is_explicit_flag,
        RANK() OVER (PARTITION BY is_explicit_flag, is_explicit ORDER BY duration_sec DESC) AS duration_sec_rank,
        ROUND(AVG(duration_sec) OVER w_release_date_year, 2) AS release_date_year_avg_duration_sec,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY YEAR(release_date))) AS distinct_artist_id_in_release_date_year
    FROM valid_tracks
    WINDOW w_is_explicit_flag_is_explicit AS (PARTITION BY is_explicit_flag, is_explicit ORDER BY duration_sec DESC, id), w_is_explicit_by_is_explicit_f_2 AS (PARTITION BY is_explicit ORDER BY is_explicit_flag, id), w_release_date_year AS (PARTITION BY YEAR(release_date))
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        distinct_artist_id_in_release_date_year,
        ROUND(SUM(distinct_artist_id_in_release_date_year) / NULLIF(SUM(first_is_explicit_flag), 0), 3) AS distinct_artist_id_in_release_date_year_to_first,
        SUM(CASE WHEN NOT is_explicit THEN 1 ELSE 0 END) AS n_not_is_explicit
    FROM ranked
    GROUP BY distinct_artist_id_in_release_date_year
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(n_not_is_explicit) OVER w_distinct_artist_id_in_releas AS distinct_artist_id_in_release_date_year_n_not_is,
        NTILE(4) OVER w_distinct_artist_id_in_releas_2 AS n_not_is_explicit_quartile,
        SUM(n_not_is_explicit) OVER w_distinct_artist_id_in_releas AS distinct_artist_id_in_release_date_year_n_not_is_2,
        MAX(n_not_is_explicit) OVER (PARTITION BY distinct_artist_id_in_release_date_year, n_not_is_explicit ORDER BY n_not_is_explicit DESC, distinct_artist_id_in_release_date_year ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS n_not_is_explicit_max_7
    FROM stats
    WINDOW w_distinct_artist_id_in_releas AS (PARTITION BY distinct_artist_id_in_release_date_year, n_not_is_explicit), w_distinct_artist_id_in_releas_2 AS (PARTITION BY distinct_artist_id_in_release_date_year, n_not_is_explicit ORDER BY n_not_is_explicit DESC, distinct_artist_id_in_release_date_year)
)
SELECT
    distinct_artist_id_in_release_date_year,
    distinct_artist_id_in_release_date_year_to_first,
    n_not_is_explicit,
    distinct_artist_id_in_release_date_year_n_not_is,
    distinct_artist_id_in_release_date_year_n_not_is_2,
    GREATEST(n_not_is_explicit, n_not_is_explicit_max_7) AS greatest_n_not_is_explicit_n_not_is_explicit_max,
    COALESCE(n_not_is_explicit_max_7, 0) AS n_not_is_explicit_max_7_filled
FROM with_windows
ORDER BY distinct_artist_id_in_release_date_year_n_not_is DESC, distinct_artist_id_in_release_date_year
LIMIT 5
