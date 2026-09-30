WITH recent AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        bpm,
        moods,
        DAYOFWEEK(release_date) AS release_date_dayofweek,
        DATE_TRUNC('QUARTER', release_date) AS release_date_quarter
    FROM tracks
    WHERE bpm BETWEEN 76 AND 180
      AND LOWER(title) LIKE '% l%'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        bpm,
        moods,
        release_date_dayofweek,
        release_date_quarter,
        MAX(bpm) OVER (PARTITION BY is_explicit, artist_id) AS is_explicit_artist_id_max_bpm,
        RANK() OVER (ORDER BY bpm ASC, id) AS bpm_rank,
        FIRST_VALUE(release_date_dayofweek) OVER (PARTITION BY artist_id, release_date_dayofweek ORDER BY bpm ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_release_date_dayofweek,
        SUM(bpm) OVER (ORDER BY bpm ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS bpm_sum_1_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_id, is_explicit ORDER BY release_date_dayofweek ASC), 3) AS release_date_dayofweek_percent_rank,
        NTH_VALUE(bpm, 2) OVER (PARTITION BY release_date_dayofweek ORDER BY release_date_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm,
        LAST_VALUE(release_date_quarter) OVER (PARTITION BY release_date_dayofweek ORDER BY release_date_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_release_date_quarter,
        FIRST_VALUE(release_date_quarter) OVER (PARTITION BY release_date_dayofweek ORDER BY release_date_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_release_date_quarter,
        ROUND(release_date_dayofweek - AVG(release_date_dayofweek) OVER (PARTITION BY is_explicit, artist_id), 2) AS release_date_dayofweek_vs_is_explicit_artist_id_,
        ROW_NUMBER() OVER (PARTITION BY artist_id, release_date_dayofweek ORDER BY bpm ASC NULLS LAST, id) AS rn,
        DENSE_RANK() OVER (PARTITION BY release_date_dayofweek ORDER BY bpm ASC, id) AS bpm_dense_rank,
        ROUND(AVG(bpm) OVER (PARTITION BY is_explicit, artist_id), 2) AS is_explicit_artist_id_avg_bpm,
        DENSE_RANK() OVER (PARTITION BY release_date_dayofweek ORDER BY bpm DESC, id) AS bpm_dense_rank_2,
        LAST_VALUE(release_date_dayofweek) OVER (PARTITION BY release_date_dayofweek ORDER BY bpm ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_release_date_dayofweek,
        FIRST_VALUE(release_date_dayofweek) IGNORE NULLS OVER (ORDER BY bpm ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_release_date_dayofweek
    FROM recent
    WINDOW w_is_explicit_artist_id AS (PARTITION BY is_explicit, artist_id)
)
SELECT
    id,
    is_explicit,
    bpm,
    release_date_quarter,
    bpm_rank,
    first_release_date_dayofweek,
    bpm_sum_1_prior,
    release_date_dayofweek_percent_rank,
    first_release_date_quarter,
    release_date_dayofweek_vs_is_explicit_artist_id_,
    rn,
    bpm_dense_rank,
    first_known_release_date_dayofweek
FROM metrics
WHERE bpm > 89
ORDER BY bpm NULLS LAST, bpm_rank
