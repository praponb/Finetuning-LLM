WITH base AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        ADD_MONTHS(release_date, 1) AS release_date_plus_1m
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'workout')
      AND YEAR(release_date) = 2024
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.moods,
        s.release_date_plus_1m,
        a.is_verified AS artist_is_verified
    FROM base s
    JOIN artists a ON a.id = s.artist_id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        release_date_plus_1m,
        MAX(duration_sec) OVER (ORDER BY release_date_plus_1m, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS duration_sec_max_3_prior,
        ROUND(AVG(duration_sec) OVER (PARTITION BY is_explicit, YEAR(release_date)), 2) AS is_explicit_release_date_year_avg_duration_sec,
        MAX(duration_sec) OVER (ORDER BY release_date_plus_1m, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS duration_sec_max_4,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_is_verified, YEAR(release_date) ORDER BY release_date ASC), 3) AS release_date_cume_dist,
        NTH_VALUE(duration_sec, 2) OVER (ORDER BY release_date_plus_1m, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec,
        NTILE(10) OVER (PARTITION BY artist_is_verified, YEAR(release_date) ORDER BY release_date ASC, id) AS release_date_decile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_is_verified, YEAR(release_date) ORDER BY release_date ASC, id), 3) AS release_date_percent_rank,
        FIRST_VALUE(release_date_plus_1m) IGNORE NULLS OVER (PARTITION BY artist_is_verified, YEAR(release_date) ORDER BY release_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_release_date_plus_1m,
        ROUND(CUME_DIST() OVER w_is_explicit_artist_is_verifi, 3) AS release_date_cume_dist_2,
        SUM(duration_sec) OVER (PARTITION BY is_explicit, YEAR(release_date)) AS is_explicit_release_date_year_sum_duration_sec
    FROM with_parent
    WINDOW w_is_explicit_release_date_yea AS (PARTITION BY is_explicit, YEAR(release_date)), w_is_explicit_artist_is_verifi AS (PARTITION BY is_explicit, artist_is_verified ORDER BY release_date ASC)
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        TRUNC(release_date_plus_1m, 'MM') AS release_date_plus_1m_month_start,
        ROUND(release_date_cume_dist_2 * 1.5, 2) AS release_date_cume_dist_2_adj,
        LAST_DAY(release_date_plus_1m) AS release_date_plus_1m_month_end
    FROM sequenced
    WHERE duration_sec_max_3_prior > 266
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        is_explicit,
        release_date_plus_1m,
        duration_sec_max_3_prior,
        is_explicit_release_date_year_avg_duration_sec,
        duration_sec_max_4,
        release_date_cume_dist,
        second_duration_sec,
        release_date_decile,
        release_date_percent_rank,
        first_known_release_date_plus_1m,
        release_date_cume_dist_2,
        is_explicit_release_date_year_sum_duration_sec,
        release_date_plus_1m_month_start,
        release_date_cume_dist_2_adj,
        COUNT(*) OVER w_release_date_plus_1m_month_e AS rows_in_release_date_plus_1m_month_end_month_art,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_id ORDER BY release_date_plus_1m DESC, id), 3) AS release_date_plus_1m_percent_rank,
        duration_sec_max_4 - LAG(duration_sec_max_4) OVER w_release_date_decile_by_relea AS duration_sec_max_4_change,
        DATEDIFF(release_date_plus_1m, LAG(release_date_plus_1m) OVER (PARTITION BY artist_id ORDER BY release_date_plus_1m DESC, id)) AS days_since_prev_release_date_plus_1m,
        ROUND(release_date_cume_dist - AVG(release_date_cume_dist) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date_plus_1m_month_end), artist_id), 2) AS release_date_cume_dist_vs_release_date_plus_1m_m,
        DENSE_RANK() OVER (PARTITION BY release_date_decile ORDER BY release_date_cume_dist_2_adj DESC NULLS FIRST, id) AS release_date_cume_dist_2_adj_dense_rank
    FROM annotated
    WINDOW w_release_date_plus_1m_month_e AS (PARTITION BY DATE_TRUNC('MONTH', release_date_plus_1m_month_end), artist_id), w_release_date_decile_by_relea AS (PARTITION BY release_date_decile ORDER BY release_date_cume_dist_2_adj DESC NULLS FIRST, id)
),
grouped AS (
    -- Summarise per group
    SELECT
        artist_id,
        is_explicit,
        release_date_cume_dist_2_adj_dense_rank,
        SUM(rows_in_release_date_plus_1m_month_end_month_art) AS total_rows_in_release_date_plus_1m_month_end_mon,
        MIN(release_date_plus_1m) AS first_release_date_plus_1m
    FROM ranked
    GROUP BY artist_id, is_explicit, release_date_cume_dist_2_adj_dense_rank
)
SELECT
    artist_id,
    release_date_cume_dist_2_adj_dense_rank,
    total_rows_in_release_date_plus_1m_month_end_mon,
    first_release_date_plus_1m
FROM grouped
ORDER BY artist_id
