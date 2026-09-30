WITH cleaned AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix
    FROM tracks
    WHERE (release_date IS NOT NULL OR duration_sec BETWEEN 142 AND 392)
),
joined_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.title_prefix,
        a.genre AS artist_genre,
        a.debut_year AS artist_debut_year
    FROM cleaned s
    JOIN artists a ON a.id = s.artist_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        title_prefix,
        artist_debut_year,
        SUM(bpm) OVER w_release_date_month AS release_date_month_sum_bpm,
        MAX(bpm) OVER (PARTITION BY is_explicit ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_bpm_so_far,
        FIRST_VALUE(bpm) IGNORE NULLS OVER (PARTITION BY YEAR(release_date), artist_genre ORDER BY artist_debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_bpm,
        ROUND(CUME_DIST() OVER w_release_date_year_artist_gen_2, 3) AS artist_debut_year_cume_dist,
        COUNT(*) OVER w_is_explicit_artist_id AS rows_in_is_explicit_artist_id,
        DATEDIFF(release_date, LAG(release_date) OVER (PARTITION BY is_explicit ORDER BY bpm, id)) AS days_since_prev_release_date,
        MIN(artist_debut_year) OVER (PARTITION BY is_explicit, artist_id) AS is_explicit_artist_id_min_artist_debut_year,
        NTH_VALUE(release_date, 2) OVER (PARTITION BY YEAR(release_date) ORDER BY bpm ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date,
        LAG(bpm, 1, 0) OVER (ORDER BY artist_debut_year ASC, id) AS prev_bpm,
        FIRST_VALUE(release_date) IGNORE NULLS OVER (PARTITION BY YEAR(release_date) ORDER BY bpm ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_release_date,
        SIZE(COLLECT_SET(artist_id) OVER w_release_date_month) AS distinct_artist_id_in_release_date_month,
        RANK() OVER w_release_date_year_by_bpm AS bpm_rank,
        SUM(artist_debut_year) OVER (PARTITION BY MONTH(release_date)) AS release_date_month_sum_artist_debut_year,
        ROUND(CUME_DIST() OVER (ORDER BY artist_debut_year ASC, id), 3) AS artist_debut_year_cume_dist_2,
        NTH_VALUE(artist_debut_year, 2) OVER (ORDER BY artist_debut_year ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_artist_debut_year
    FROM joined_artist
    WINDOW w_release_date_month AS (PARTITION BY MONTH(release_date)), w_release_date_year_artist_gen_2 AS (PARTITION BY YEAR(release_date), artist_genre ORDER BY artist_debut_year DESC, id), w_is_explicit_artist_id AS (PARTITION BY is_explicit, artist_id), w_release_date_year_by_bpm AS (PARTITION BY YEAR(release_date) ORDER BY bpm ASC NULLS LAST, id)
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        artist_debut_year,
        release_date_month_sum_bpm,
        max_bpm_so_far,
        first_known_bpm,
        artist_debut_year_cume_dist,
        days_since_prev_release_date,
        is_explicit_artist_id_min_artist_debut_year,
        second_release_date,
        prev_bpm,
        first_known_release_date,
        distinct_artist_id_in_release_date_month,
        bpm_rank,
        release_date_month_sum_artist_debut_year,
        artist_debut_year_cume_dist_2,
        second_artist_debut_year,
        DATE_TRUNC('QUARTER', first_known_release_date) AS first_known_release_date_quarter,
        DATEDIFF(DATE '2025-12-31', release_date) AS days_since_release_date,
        ABS(max_bpm_so_far - 1104) AS max_bpm_so_far_distance
    FROM windowed
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(max_bpm_so_far) OVER (PARTITION BY bpm_rank ORDER BY release_date_month_sum_bpm, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS max_bpm_so_far_max_2,
        ROUND(bpm_rank - AVG(bpm_rank) OVER (PARTITION BY YEAR(first_known_release_date)), 2) AS bpm_rank_vs_first_known_release_date_year_avg,
        MIN(max_bpm_so_far_distance) OVER (PARTITION BY YEAR(first_known_release_date)) AS first_known_release_date_year_min_max_bpm_so_far,
        ROUND(CUME_DIST() OVER (ORDER BY first_known_release_date NULLS LAST), 3) AS first_known_release_date_cume_dist,
        LAST_VALUE(first_known_bpm) OVER (PARTITION BY bpm_rank ORDER BY release_date_month_sum_bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_first_known_bpm,
        NTH_VALUE(bpm_rank, 2) OVER (ORDER BY first_known_release_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm_rank,
        ROUND(AVG(max_bpm_so_far_distance) OVER (ORDER BY first_known_release_date NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS max_bpm_so_far_distance_avg_6_prior
    FROM scored
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        bpm_rank,
        distinct_artist_id_in_release_date_month,
        TO_DATE(first_known_release_date_quarter) AS first_known_release_date_quarter_day,
        ROUND(STDDEV(last_first_known_bpm), 2) AS stddev_last_first_known_bpm,
        COUNT(*) AS row_count,
        ROUND(STDDEV(max_bpm_so_far_distance), 2) AS stddev_max_bpm_so_far_distance,
        SUM(release_date_month_sum_artist_debut_year) AS total_release_date_month_sum_artist_debut_year
    FROM sequenced
    GROUP BY bpm_rank, distinct_artist_id_in_release_date_month, TO_DATE(first_known_release_date_quarter)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        bpm_rank,
        distinct_artist_id_in_release_date_month,
        first_known_release_date_quarter_day,
        stddev_last_first_known_bpm,
        row_count,
        ROUND(CUME_DIST() OVER (PARTITION BY row_count ORDER BY row_count DESC), 3) AS row_count_cume_dist,
        bpm_rank - LAG(bpm_rank) OVER w_row_count_by_row_count AS bpm_rank_change,
        LEAD(stddev_last_first_known_bpm, 3) OVER w_row_count_by_row_count AS next3_stddev_last_first_known_bpm
    FROM summary
    WINDOW w_row_count_by_row_count AS (PARTITION BY row_count ORDER BY row_count DESC, bpm_rank, distinct_artist_id_in_release_date_month, first_known_release_date_quarter_day)
)
SELECT
    bpm_rank,
    distinct_artist_id_in_release_date_month,
    first_known_release_date_quarter_day,
    stddev_last_first_known_bpm,
    row_count,
    row_count_cume_dist,
    bpm_rank_change,
    next3_stddev_last_first_known_bpm,
    COALESCE(bpm_rank, 0) AS bpm_rank_filled
FROM metrics
ORDER BY bpm_rank_change DESC
