WITH recent_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality
    FROM meter_readings
    WHERE site_id >= 8
      AND quality IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(kwh) OVER (PARTITION BY quality, MONTH(read_at) ORDER BY read_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_kwh,
        MAX(kwh) OVER (PARTITION BY quality, site_id ORDER BY kwh, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS kwh_max_6_prior,
        ROW_NUMBER() OVER w_quality_site_id_by_kwh AS rn,
        ROUND(CUME_DIST() OVER (PARTITION BY quality, MONTH(read_at) ORDER BY read_at DESC), 3) AS read_at_cume_dist,
        ROUND(AVG(kwh) OVER (PARTITION BY quality, MONTH(read_at) ORDER BY read_at DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS kwh_avg_2,
        DENSE_RANK() OVER (PARTITION BY quality, site_id ORDER BY kwh, id) AS kwh_dense_rank,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY quality, site_id), 0), 4) AS kwh_share_of_quality_site_id,
        MAX(kwh) OVER (PARTITION BY quality, site_id ORDER BY kwh DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS kwh_max_1_prior,
        NTILE(4) OVER (PARTITION BY quality, site_id ORDER BY kwh, id) AS kwh_quartile,
        DENSE_RANK() OVER (PARTITION BY quality, MONTH(read_at) ORDER BY read_at DESC) AS read_at_dense_rank,
        COUNT(*) OVER w_quality_site_id AS rows_in_quality_site_id,
        ROW_NUMBER() OVER w_quality_site_id_by_kwh_2 AS kwh_seq,
        ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(LAG(read_at) OVER (PARTITION BY quality, site_id ORDER BY kwh, id))) / 60, 1) AS minutes_since_prev_read_at,
        COUNT(*) OVER (PARTITION BY quality, site_id ORDER BY kwh DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        NTH_VALUE(kwh, 2) OVER (PARTITION BY quality, site_id ORDER BY kwh, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_kwh
    FROM recent_readings
    WINDOW w_quality_site_id_by_kwh AS (PARTITION BY quality, site_id ORDER BY kwh, id), w_quality_site_id AS (PARTITION BY quality, site_id), w_quality_site_id_by_kwh_2 AS (PARTITION BY quality, site_id ORDER BY kwh DESC NULLS FIRST, id)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        kwh,
        quality,
        first_kwh,
        kwh_max_6_prior,
        rn,
        kwh_avg_2,
        kwh_max_1_prior,
        kwh_quartile,
        rows_in_quality_site_id,
        kwh_seq,
        minutes_since_prev_read_at,
        running_count,
        ROUND(AVG(kwh_avg_2) OVER w_rn, 2) AS rn_avg_kwh_avg_2,
        kwh_max_6_prior - LAG(kwh_max_6_prior) OVER w_kwh_quartile_kwh_seq_by_kwh AS kwh_max_6_prior_change,
        kwh_dense_rank - LAG(kwh_dense_rank) OVER (PARTITION BY kwh_quartile, quality ORDER BY kwh_seq ASC, id) AS kwh_dense_rank_change,
        ROUND(AVG(kwh_max_1_prior) OVER (PARTITION BY kwh_dense_rank, site_id ORDER BY rn DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS kwh_max_1_prior_avg_7,
        ROW_NUMBER() OVER (PARTITION BY kwh_quartile, quality ORDER BY kwh_seq ASC, id) AS kwh_seq_seq,
        DENSE_RANK() OVER (PARTITION BY kwh_quartile, kwh_seq ORDER BY kwh_seq) AS kwh_seq_dense_rank
    FROM metrics
    WINDOW w_rn AS (PARTITION BY rn), w_kwh_quartile_kwh_seq_by_kwh AS (PARTITION BY kwh_quartile, kwh_seq ORDER BY kwh_seq, id)
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    first_kwh,
    rn,
    kwh_avg_2,
    kwh_max_1_prior,
    kwh_quartile,
    rows_in_quality_site_id,
    minutes_since_prev_read_at,
    running_count,
    rn_avg_kwh_avg_2,
    kwh_max_6_prior_change,
    kwh_dense_rank_change,
    kwh_max_1_prior_avg_7,
    kwh_seq_seq,
    kwh_seq_dense_rank,
    LOWER(REGEXP_REPLACE(TRIM(quality), ' ', '_')) AS quality_slug,
    LENGTH(TRIM(quality)) AS quality_len
FROM with_windows
ORDER BY site_id, kwh_seq_dense_rank, id
LIMIT 5
