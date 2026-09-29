WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.site_id,
        a.read_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.kwh) AS prior_kwh_1d
    FROM meter_readings a
    LEFT JOIN meter_readings b
      ON b.site_id = a.site_id
     AND b.read_at < a.read_at
     AND b.read_at >= a.read_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.site_id, a.read_at
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        read_at,
        prior_rows_1d,
        FIRST_VALUE(prior_kwh_1d) IGNORE NULLS OVER (PARTITION BY MONTH(read_at), site_id ORDER BY prior_kwh_1d DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_prior_kwh_1d,
        SUM(prior_kwh_1d) OVER (PARTITION BY MONTH(read_at), site_id ORDER BY prior_kwh_1d DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS prior_kwh_1d_sum_1_prior
    FROM matched
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        read_at,
        prior_rows_1d,
        prior_kwh_1d_sum_1_prior,
        MAX(prior_rows_1d) OVER (PARTITION BY prior_rows_1d ORDER BY first_known_prior_kwh_1d ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS prior_rows_1d_max_6_prior,
        ROW_NUMBER() OVER (PARTITION BY prior_rows_1d ORDER BY first_known_prior_kwh_1d ASC, id) AS seq_no,
        ROUND(PERCENT_RANK() OVER w_prior_rows_1d_by_first_known_3, 3) AS first_known_prior_kwh_1d_percent_rank,
        DENSE_RANK() OVER w_prior_rows_1d_by_first_known_2 AS first_known_prior_kwh_1d_dense_rank
    FROM windowed
    WINDOW w_prior_rows_1d_by_first_known_3 AS (PARTITION BY prior_rows_1d ORDER BY first_known_prior_kwh_1d ASC), w_prior_rows_1d_by_first_known_2 AS (PARTITION BY prior_rows_1d ORDER BY first_known_prior_kwh_1d ASC, id)
)
-- Final projection
SELECT
    id,
    read_at,
    prior_rows_1d,
    prior_kwh_1d_sum_1_prior,
    seq_no,
    first_known_prior_kwh_1d_percent_rank,
    first_known_prior_kwh_1d_dense_rank
FROM with_trends
WHERE prior_rows_1d_max_6_prior < 17
ORDER BY first_known_prior_kwh_1d_dense_rank DESC
