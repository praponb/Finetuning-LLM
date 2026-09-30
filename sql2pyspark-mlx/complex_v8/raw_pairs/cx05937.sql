WITH cleaned AS (
    -- Scope the policyholders rows
    SELECT
        id,
        region,
        risk_score,
        joined_on,
        segment,
        ROUND(risk_score * 1.1, 2) AS risk_score_adj,
        ROUND(risk_score * 0.25, 2) AS risk_score_adj_2
    FROM policyholders
    WHERE risk_score >= 0.2
),
stats AS (
    -- Group-level totals
    SELECT
        region,
        COUNT(*) AS n_rows,
        MIN(risk_score) AS min_risk_score,
        MIN(risk_score_adj_2) AS min_risk_score_adj_2,
        COUNT(*) FILTER (WHERE segment = 'VIP') AS n_segment_vip,
        ROUND(AVG(risk_score_adj), 2) AS avg_risk_score_adj
    FROM cleaned
    GROUP BY region
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(region, '#', CAST(n_rows AS STRING)) AS region_ref,
        CONCAT(region, '#', CAST(n_rows AS STRING)) AS region_ref_2,
        CONCAT(region, '#', CAST(n_segment_vip AS STRING)) AS region_ref_3
    FROM stats
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(min_risk_score_adj_2 - AVG(min_risk_score_adj_2) OVER (PARTITION BY region, n_rows), 2) AS min_risk_score_adj_2_vs_region_n_rows_avg,
        DENSE_RANK() OVER w_region_n_segment_vip_by_min AS min_risk_score_dense_rank,
        NTH_VALUE(n_rows, 2) OVER (PARTITION BY region, n_segment_vip ORDER BY min_risk_score ASC, region ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_n_rows,
        SUM(min_risk_score_adj_2) OVER (PARTITION BY region, n_segment_vip ORDER BY min_risk_score ASC, region ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS min_risk_score_adj_2_sum_1_prior,
        RANK() OVER (PARTITION BY n_segment_vip ORDER BY min_risk_score_adj_2) AS min_risk_score_adj_2_rank
    FROM scored
    WINDOW w_region_n_segment_vip_by_min AS (PARTITION BY region, n_segment_vip ORDER BY min_risk_score ASC)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        region,
        n_rows,
        min_risk_score,
        n_segment_vip,
        avg_risk_score_adj,
        region_ref,
        region_ref_3,
        min_risk_score_dense_rank,
        second_n_rows,
        min_risk_score_adj_2_sum_1_prior,
        min_risk_score_adj_2_rank,
        DENSE_RANK() OVER (PARTITION BY second_n_rows, n_segment_vip ORDER BY min_risk_score_dense_rank) AS min_risk_score_dense_rank_dense_rank,
        ROUND(min_risk_score_adj_2 - AVG(min_risk_score_adj_2) OVER (PARTITION BY min_risk_score_adj_2_rank, min_risk_score_dense_rank), 2) AS min_risk_score_adj_2_vs_min_risk_score_adj_2_ran
    FROM windowed
),
scored_2 AS (
    -- Label and score rows
    SELECT
        region,
        n_segment_vip,
        avg_risk_score_adj,
        region_ref,
        region_ref_3,
        min_risk_score_dense_rank,
        min_risk_score_adj_2_rank,
        min_risk_score_adj_2_vs_min_risk_score_adj_2_ran,
        CASE WHEN n_segment_vip >= 8 THEN 'large' WHEN n_segment_vip >= 4 THEN 'medium' ELSE 'small' END AS n_segment_vip_tier,
        ROUND(min_risk_score * 0.9, 2) AS min_risk_score_adj,
        COALESCE(region, 'UNKNOWN') AS region_or_unknown
    FROM ranked
)
SELECT
    region,
    n_segment_vip,
    avg_risk_score_adj,
    region_ref,
    region_ref_3,
    min_risk_score_adj_2_rank,
    min_risk_score_adj_2_vs_min_risk_score_adj_2_ran,
    n_segment_vip_tier,
    region_or_unknown
FROM scored_2
ORDER BY min_risk_score_adj_2_vs_min_risk_score_adj_2_ran DESC
