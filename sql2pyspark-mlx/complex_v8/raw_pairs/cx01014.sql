WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM tracks
    WHERE title IS NOT NULL
),
joined_artist AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        a.genre AS artist_genre,
        a.artist_name,
        a.country AS artist_country
    FROM prepared s
    LEFT JOIN artists a ON a.id = s.artist_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        artist_genre,
        artist_country,
        MAX(duration_sec) OVER (ORDER BY duration_sec DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS duration_sec_max_7,
        ROW_NUMBER() OVER (PARTITION BY artist_genre, YEAR(release_date) ORDER BY duration_sec DESC, id) AS duration_sec_seq,
        LAG(release_date, 2) OVER (ORDER BY duration_sec DESC, id) AS prev2_release_date,
        NTILE(5) OVER (ORDER BY duration_sec, id) AS duration_sec_quintile,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY is_explicit, DATE_TRUNC('MONTH', release_date)), 0), 4) AS bpm_share_of_is_explicit_release_date_month,
        ROUND(CUME_DIST() OVER w_artist_genre_release_date_ye_2, 3) AS duration_sec_cume_dist,
        MAX(duration_sec) OVER (PARTITION BY artist_genre, YEAR(release_date) ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_duration_sec_so_far,
        RANK() OVER w_all_by_duration_sec_ties AS duration_sec_rank,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY is_explicit), 0), 4) AS duration_sec_share_of_is_explicit,
        ROUND(PERCENT_RANK() OVER (ORDER BY duration_sec DESC, id), 3) AS duration_sec_percent_rank,
        duration_sec - LAG(duration_sec) OVER w_artist_country_by_release_da AS duration_sec_change,
        NTH_VALUE(duration_sec, 2) OVER (ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec
    FROM joined_artist
    WINDOW w_artist_genre_release_date_ye_2 AS (PARTITION BY artist_genre, YEAR(release_date) ORDER BY duration_sec DESC), w_all_by_duration_sec_ties AS (ORDER BY duration_sec DESC), w_artist_country_by_release_da AS (PARTITION BY artist_country ORDER BY release_date DESC NULLS FIRST, id)
),
leaders AS (
    -- First row per group
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        artist_genre,
        artist_country,
        duration_sec_max_7,
        prev2_release_date,
        duration_sec_quintile,
        bpm_share_of_is_explicit_release_date_month,
        duration_sec_cume_dist,
        max_duration_sec_so_far,
        duration_sec_rank,
        duration_sec_share_of_is_explicit,
        duration_sec_percent_rank,
        duration_sec_change,
        second_duration_sec
    FROM ranked
    WHERE duration_sec_seq = 1
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY duration_sec_percent_rank DESC, id) AS seq_no,
        NTILE(5) OVER (PARTITION BY artist_id ORDER BY duration_sec_percent_rank DESC, id) AS duration_sec_percent_rank_quintile,
        MAX(max_duration_sec_so_far) OVER (PARTITION BY artist_genre) AS artist_genre_max_max_duration_sec_so_far,
        RANK() OVER (ORDER BY release_date ASC, id) AS release_date_rank,
        MIN(duration_sec_percent_rank) OVER (PARTITION BY artist_genre) AS artist_genre_min_duration_sec_percent_rank
    FROM leaders
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        release_date_rank,
        CASE WHEN duration_sec_max_7 >= 909 THEN 'large' WHEN duration_sec_max_7 >= 558 THEN 'medium' ELSE 'small' END AS duration_sec_max_7_tier,
        artist_genre,
        SUM(duration_sec_percent_rank) AS total_duration_sec_percent_rank,
        SUM(duration_sec_rank) FILTER (WHERE NOT is_explicit) AS duration_sec_rank_not_is_explicit,
        COUNT(*) FILTER (WHERE artist_genre <> 'Pop') AS n_artist_genre_pop,
        COUNT(DISTINCT artist_genre) AS distinct_artist_genres
    FROM metrics
    WHERE release_date_rank >= 2
    GROUP BY release_date_rank, CASE WHEN duration_sec_max_7 >= 909 THEN 'large' WHEN duration_sec_max_7 >= 558 THEN 'medium' ELSE 'small' END, artist_genre
    HAVING SUM(duration_sec_quintile) > 4
)
SELECT
    release_date_rank,
    duration_sec_max_7_tier,
    artist_genre,
    total_duration_sec_percent_rank,
    n_artist_genre_pop,
    distinct_artist_genres
FROM totals
ORDER BY artist_genre DESC NULLS FIRST
