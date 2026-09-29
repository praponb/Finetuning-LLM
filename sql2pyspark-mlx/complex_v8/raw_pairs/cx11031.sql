WITH valid AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        moods,
        ADD_MONTHS(release_date, 12) AS release_date_plus_12m
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'E%'
),
latest_per_key AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        moods,
        release_date_plus_12m
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY release_date DESC, id DESC) AS recency_rank
        FROM valid
    ) ranked
    WHERE recency_rank = 1
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(release_date_plus_12m, 2) OVER (PARTITION BY is_explicit ORDER BY release_date DESC, id) AS prev2_release_date_plus_12m,
        DENSE_RANK() OVER (PARTITION BY is_explicit ORDER BY release_date DESC, id) AS release_date_dense_rank,
        NTH_VALUE(release_date_plus_12m, 2) OVER (PARTITION BY is_explicit ORDER BY release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date_plus_12m
    FROM latest_per_key
)
SELECT
    is_explicit,
    release_date_dense_rank,
    artist_id,
    SUM(release_date_dense_rank) AS total_release_date_dense_rank
FROM windowed
GROUP BY GROUPING SETS ((is_explicit, release_date_dense_rank, artist_id), (is_explicit), (release_date_dense_rank), ())
ORDER BY is_explicit NULLS LAST, release_date_dense_rank NULLS LAST, artist_id NULLS LAST
