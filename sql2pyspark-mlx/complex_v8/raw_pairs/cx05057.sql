WITH prepared AS (
    -- Filter artists
    SELECT
        id,
        artist_name,
        genre,
        debut_year,
        is_verified,
        CASE WHEN genre IN ('Jazz', 'Pop') THEN 'group_a' WHEN genre = 'Indie' THEN 'group_b' ELSE 'other' END AS genre_group
    FROM artists
    WHERE (debut_year < 2020 OR country = 'KR')
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        genre,
        debut_year,
        is_verified,
        genre_group,
        LAST_VALUE(debut_year) IGNORE NULLS OVER (ORDER BY debut_year ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_debut_year,
        SUM(debut_year) OVER (ORDER BY debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_debut_year,
        RANK() OVER w_genre_by_debut_year AS debut_year_rank,
        LAG(debut_year) OVER (PARTITION BY genre_group ORDER BY debut_year, id) AS prev_debut_year,
        MAX(debut_year) OVER w_genre AS genre_max_debut_year,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_verified, genre_group ORDER BY debut_year ASC, id), 3) AS debut_year_percent_rank,
        ROUND(AVG(debut_year) OVER (PARTITION BY genre_group ORDER BY debut_year, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS debut_year_avg_3_prior,
        LAST_VALUE(debut_year) IGNORE NULLS OVER (PARTITION BY genre ORDER BY debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_debut_year_2,
        LAST_VALUE(debut_year) OVER (ORDER BY debut_year ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_debut_year,
        COUNT(*) OVER (PARTITION BY is_verified, genre) AS rows_in_is_verified_genre,
        SIZE(COLLECT_SET(genre_group) OVER w_is_verified_genre) AS distinct_genre_group_in_is_verified_genre
    FROM prepared
    WINDOW w_genre_by_debut_year AS (PARTITION BY genre ORDER BY debut_year DESC, id), w_genre AS (PARTITION BY genre), w_is_verified_genre AS (PARTITION BY is_verified, genre)
),
first_rows AS (
    -- Top 3 per group
    SELECT
        id,
        genre,
        debut_year,
        is_verified,
        genre_group,
        last_known_debut_year,
        running_debut_year,
        prev_debut_year,
        genre_max_debut_year,
        debut_year_percent_rank,
        debut_year_avg_3_prior,
        last_known_debut_year_2,
        last_debut_year,
        rows_in_is_verified_genre,
        distinct_genre_group_in_is_verified_genre
    FROM with_trends
    WHERE debut_year_rank <= 3
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        genre,
        debut_year,
        is_verified,
        genre_group,
        running_debut_year,
        prev_debut_year,
        genre_max_debut_year,
        last_known_debut_year_2,
        last_debut_year,
        rows_in_is_verified_genre,
        COUNT(*) OVER w_genre_group_is_verified AS rows_in_genre_group_is_verified,
        MIN(debut_year_avg_3_prior) OVER w_genre_group_is_verified AS genre_group_is_verified_min_debut_year_avg_3_pri,
        MAX(last_known_debut_year_2) OVER (PARTITION BY distinct_genre_group_in_is_verified_genre ORDER BY debut_year_avg_3_prior ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_last_known_debut_year_2_so_far,
        ROUND(AVG(last_known_debut_year_2) OVER w_genre_group_is_verified, 2) AS genre_group_is_verified_avg_last_known_debut_yea,
        NTILE(3) OVER (PARTITION BY distinct_genre_group_in_is_verified_genre ORDER BY debut_year_avg_3_prior ASC, id) AS debut_year_avg_3_prior_tercile,
        NTH_VALUE(running_debut_year, 2) OVER (PARTITION BY distinct_genre_group_in_is_verified_genre ORDER BY debut_year_avg_3_prior ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_running_debut_year,
        MAX(rows_in_is_verified_genre) OVER (PARTITION BY genre_group, is_verified) AS genre_group_is_verified_max_rows_in_is_verified_
    FROM first_rows
    WINDOW w_genre_group_is_verified AS (PARTITION BY genre_group, is_verified)
)
SELECT
    id,
    debut_year,
    is_verified,
    genre_group,
    running_debut_year,
    prev_debut_year,
    genre_max_debut_year,
    last_known_debut_year_2,
    rows_in_is_verified_genre,
    rows_in_genre_group_is_verified,
    genre_group_is_verified_min_debut_year_avg_3_pri,
    max_last_known_debut_year_2_so_far,
    genre_group_is_verified_avg_last_known_debut_yea,
    second_running_debut_year,
    genre_group_is_verified_max_rows_in_is_verified_
FROM with_windows
ORDER BY id DESC, running_debut_year DESC
