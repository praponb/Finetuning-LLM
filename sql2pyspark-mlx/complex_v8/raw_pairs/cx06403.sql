WITH base AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        moods,
        COALESCE(duration_sec, 0) AS duration_sec_filled
    FROM tracks
    WHERE is_explicit = TRUE
      AND artist_id IS NOT NULL
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        title,
        duration_sec,
        duration_sec_filled,
        NTILE(5) OVER (ORDER BY duration_sec_filled DESC, id) AS duration_sec_filled_quintile,
        ROUND(AVG(duration_sec_filled) OVER (PARTITION BY artist_id), 2) AS artist_id_avg_duration_sec_filled,
        ROW_NUMBER() OVER (ORDER BY duration_sec_filled DESC, id) AS rn,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY artist_id), 2) AS duration_sec_vs_artist_id_avg,
        RANK() OVER (PARTITION BY artist_id ORDER BY duration_sec_filled DESC, id) AS duration_sec_filled_rank,
        duration_sec - LAG(duration_sec) OVER (PARTITION BY artist_id ORDER BY duration_sec_filled ASC NULLS LAST, id) AS duration_sec_change,
        duration_sec_filled - LAG(duration_sec_filled) OVER (PARTITION BY artist_id ORDER BY duration_sec_filled DESC, id) AS duration_sec_filled_change,
        ROUND(duration_sec_filled - AVG(duration_sec_filled) OVER (PARTITION BY artist_id), 2) AS duration_sec_filled_vs_artist_id_avg,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER w_artist_id, 0), 4) AS duration_sec_share_of_artist_id,
        SUM(duration_sec_filled) OVER (ORDER BY duration_sec, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS duration_sec_filled_sum_6_prior,
        DENSE_RANK() OVER (PARTITION BY artist_id ORDER BY duration_sec_filled DESC, id) AS duration_sec_filled_dense_rank,
        SUM(duration_sec_filled) OVER (PARTITION BY artist_id ORDER BY duration_sec_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec_filled,
        MAX(duration_sec_filled) OVER (PARTITION BY artist_id) AS artist_id_max_duration_sec_filled,
        MIN(duration_sec_filled) OVER (PARTITION BY artist_id) AS artist_id_min_duration_sec_filled,
        ROUND(PERCENT_RANK() OVER (ORDER BY duration_sec), 3) AS duration_sec_percent_rank
    FROM base
    WINDOW w_artist_id AS (PARTITION BY artist_id)
),
leaders AS (
    -- Top 3 per group
    SELECT *
    FROM ranked
    WHERE rn <= 3
      AND artist_id_avg_duration_sec_filled < 321.0
)
SELECT
    id,
    title,
    duration_sec,
    duration_sec_filled,
    duration_sec_filled_quintile,
    artist_id_avg_duration_sec_filled,
    rn,
    duration_sec_filled_rank,
    duration_sec_change,
    duration_sec_filled_change,
    duration_sec_filled_vs_artist_id_avg,
    duration_sec_share_of_artist_id,
    duration_sec_filled_sum_6_prior,
    duration_sec_filled_dense_rank,
    running_duration_sec_filled,
    artist_id_max_duration_sec_filled,
    artist_id_min_duration_sec_filled,
    duration_sec_percent_rank,
    ROUND(duration_sec / NULLIF(duration_sec_filled_rank, 0), 3) AS duration_sec_per_duration_sec_filled_rank,
    LEAST(duration_sec, rn) AS least_duration_sec_rn
FROM leaders
WHERE artist_id_min_duration_sec_filled < 3172
ORDER BY artist_id_max_duration_sec_filled NULLS LAST
