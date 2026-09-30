WITH cleaned_holders AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        joined_on,
        segment,
        CASE WHEN risk_score >= 0.6 THEN 'A' WHEN risk_score >= 0.3 THEN 'B' ELSE 'C' END AS risk_score_tier
    FROM policyholders
    WHERE (segment <> 'SME' OR region IN ('Central', 'East', 'West'))
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY risk_score_tier, segment ORDER BY joined_on DESC, id) AS joined_on_dense_rank,
        LAST_VALUE(joined_on) OVER (PARTITION BY risk_score_tier ORDER BY joined_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_joined_on,
        ROUND(PERCENT_RANK() OVER (PARTITION BY risk_score_tier ORDER BY joined_on DESC), 3) AS joined_on_percent_rank,
        LAST_VALUE(joined_on) OVER (PARTITION BY risk_score_tier, segment ORDER BY joined_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_joined_on_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY risk_score_tier, segment ORDER BY joined_on DESC, id), 3) AS joined_on_percent_rank_2,
        FIRST_VALUE(joined_on) OVER (PARTITION BY risk_score_tier ORDER BY joined_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_joined_on,
        ROW_NUMBER() OVER (PARTITION BY risk_score_tier ORDER BY joined_on DESC, id) AS rn,
        ROUND(PERCENT_RANK() OVER (PARTITION BY risk_score_tier ORDER BY joined_on ASC), 3) AS joined_on_percent_rank_3,
        LAG(joined_on) OVER (PARTITION BY risk_score_tier, segment ORDER BY joined_on DESC, id) AS prev_joined_on,
        FIRST_VALUE(joined_on) IGNORE NULLS OVER (PARTITION BY risk_score_tier, segment ORDER BY joined_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_joined_on,
        ROUND(CUME_DIST() OVER (PARTITION BY risk_score_tier ORDER BY joined_on ASC), 3) AS joined_on_cume_dist,
        NTILE(4) OVER w_risk_score_tier_by_joined_on_6 AS joined_on_quartile
    FROM cleaned_holders
    WINDOW w_risk_score_tier_by_joined_on_6 AS (PARTITION BY risk_score_tier ORDER BY joined_on ASC, id)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(segment) OVER (PARTITION BY joined_on_quartile)) AS distinct_segment_in_joined_on_quartile,
        COUNT(*) OVER (PARTITION BY joined_on_quartile) AS rows_in_joined_on_quartile,
        ROUND(joined_on_cume_dist - AVG(joined_on_cume_dist) OVER (PARTITION BY joined_on_quartile), 2) AS joined_on_cume_dist_vs_joined_on_quartile_avg,
        ROUND(joined_on_quartile / NULLIF(SUM(joined_on_quartile) OVER (PARTITION BY joined_on_quartile), 0), 4) AS joined_on_quartile_share_of_joined_on_quartile,
        ROUND(joined_on_percent_rank_2 / NULLIF(SUM(joined_on_percent_rank_2) OVER (PARTITION BY joined_on_quartile), 0), 4) AS joined_on_percent_rank_2_share_of_joined_on_quar,
        ROUND(AVG(joined_on_percent_rank) OVER (ORDER BY joined_on_dense_rank, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS joined_on_percent_rank_avg_7
    FROM with_windows
)
-- Final projection
SELECT
    id,
    holder_name,
    joined_on,
    segment,
    risk_score_tier,
    joined_on_dense_rank,
    last_joined_on,
    last_joined_on_2,
    first_joined_on,
    rn,
    prev_joined_on,
    first_known_joined_on,
    joined_on_cume_dist,
    distinct_segment_in_joined_on_quartile,
    rows_in_joined_on_quartile,
    joined_on_cume_dist_vs_joined_on_quartile_avg,
    joined_on_quartile_share_of_joined_on_quartile,
    joined_on_percent_rank_avg_7,
    DATE_FORMAT(first_known_joined_on, 'yyyy-MM-dd') AS first_known_joined_on_label
FROM with_trends
ORDER BY risk_score_tier DESC
