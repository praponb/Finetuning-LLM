WITH base AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        ROUND(risk_score * 1.07, 2) AS risk_score_adj
    FROM policyholders
    WHERE YEAR(joined_on) = 2025
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(risk_score_adj / NULLIF(SUM(risk_score_adj) OVER (PARTITION BY region), 0), 4) AS risk_score_adj_share_of_region,
        COUNT(*) OVER (PARTITION BY region) AS rows_in_region,
        FIRST_VALUE(risk_score_adj) OVER (PARTITION BY region ORDER BY risk_score_adj DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_risk_score_adj,
        ROUND(risk_score / NULLIF(SUM(risk_score) OVER w_region, 0), 4) AS risk_score_share_of_region,
        ROUND(CUME_DIST() OVER (PARTITION BY region ORDER BY risk_score_adj ASC), 3) AS risk_score_adj_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY risk_score_adj ASC, id) AS seq_no,
        ROUND(risk_score - AVG(risk_score) OVER w_region, 2) AS risk_score_vs_region_avg,
        LAST_VALUE(risk_score_adj) IGNORE NULLS OVER (ORDER BY risk_score, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_risk_score_adj,
        DENSE_RANK() OVER (PARTITION BY region ORDER BY risk_score, id) AS risk_score_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY region ORDER BY risk_score), 3) AS risk_score_cume_dist,
        ROUND(CUME_DIST() OVER (ORDER BY risk_score, id), 3) AS risk_score_cume_dist_2,
        MIN(risk_score) OVER (PARTITION BY region ORDER BY risk_score_adj DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_risk_score_so_far
    FROM base
    WINDOW w_region AS (PARTITION BY region), w_region_by_risk_score_adj AS (PARTITION BY region ORDER BY risk_score_adj ASC, id)
)
-- Final projection
SELECT
    id,
    region,
    risk_score,
    risk_score_adj,
    risk_score_adj_share_of_region,
    first_risk_score_adj,
    risk_score_adj_cume_dist,
    seq_no,
    risk_score_vs_region_avg,
    last_known_risk_score_adj,
    risk_score_dense_rank,
    risk_score_cume_dist,
    risk_score_cume_dist_2,
    min_risk_score_so_far,
    LEAST(first_risk_score_adj, rows_in_region) AS least_first_risk_score_adj_rows_in_region
FROM with_trends
WHERE rows_in_region < 17
ORDER BY last_known_risk_score_adj DESC NULLS FIRST, first_risk_score_adj DESC NULLS FIRST
