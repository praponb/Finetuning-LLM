WITH filtered AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref
    FROM policies
    WHERE start_date >= DATE '2024-12-02'
      AND status = 'EXPIRED'
),
enriched_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.status,
        s.premium_bucket,
        s.status_ref,
        p.segment AS holder_segment,
        p.region AS holder_region,
        p.risk_score AS holder_risk_score
    FROM filtered s
    JOIN policyholders p ON p.id = s.holder_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(holder_risk_score - AVG(holder_risk_score) OVER w_start_date_year, 2) AS holder_risk_score_vs_start_date_year_avg,
        ROUND(AVG(premium_bucket) OVER (PARTITION BY holder_segment ORDER BY premium_bucket ASC NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS premium_bucket_avg_1_prior,
        ROUND(premium_bucket - AVG(premium_bucket) OVER (PARTITION BY YEAR(start_date)), 2) AS premium_bucket_vs_start_date_year_avg,
        FIRST_VALUE(holder_risk_score) OVER (PARTITION BY MONTH(start_date), holder_region ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_holder_risk_score,
        ROUND(AVG(premium_bucket) OVER w_start_date_year, 2) AS start_date_year_avg_premium_bucket,
        SUM(premium_bucket) OVER (PARTITION BY YEAR(start_date)) AS start_date_year_sum_premium_bucket,
        ROW_NUMBER() OVER (ORDER BY holder_risk_score DESC, id) AS seq_no,
        ROUND(AVG(holder_risk_score) OVER w_start_date_year, 2) AS start_date_year_avg_holder_risk_score,
        SUM(holder_risk_score) OVER (PARTITION BY holder_segment ORDER BY premium_bucket ASC NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS holder_risk_score_sum_1_prior,
        DENSE_RANK() OVER (PARTITION BY holder_segment ORDER BY premium_bucket ASC NULLS LAST) AS premium_bucket_dense_rank,
        ROUND(AVG(premium_bucket) OVER (PARTITION BY MONTH(start_date), holder_region ORDER BY end_date, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS premium_bucket_avg_1_prior_2
    FROM enriched_holder
    WINDOW w_start_date_year AS (PARTITION BY YEAR(start_date))
),
summary AS (
    -- Summarise per group
    SELECT
        premium_bucket_dense_rank,
        seq_no,
        ROUND(AVG(premium_bucket_vs_start_date_year_avg), 2) AS avg_premium_bucket_vs_start_date_year_avg,
        COUNT(*) AS n_rows
    FROM with_windows
    WHERE premium_bucket_avg_1_prior_2 < 3560.0
    GROUP BY premium_bucket_dense_rank, seq_no
    HAVING COUNT(*) >= 3
)
-- Final projection
SELECT
    premium_bucket_dense_rank,
    seq_no,
    n_rows,
    FLOOR(n_rows / 2) * 2 AS n_rows_bucket
FROM summary
ORDER BY n_rows DESC
