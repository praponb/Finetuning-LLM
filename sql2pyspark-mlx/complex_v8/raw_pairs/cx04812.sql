WITH cleaned_holders AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment
    FROM policyholders
    WHERE segment NOT IN ('RETAIL', 'VIP')
      AND risk_score BETWEEN 0.1 AND 0.7
),
deduped AS (
    -- Keep the earliest row per region
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY region ORDER BY joined_on, id) AS recency_rank
        FROM cleaned_holders
    ) ranked
    WHERE recency_rank = 1
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER w_region_segment_by_joined_on AS joined_on_rank,
        FIRST_VALUE(joined_on) OVER (PARTITION BY region, segment ORDER BY joined_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_joined_on,
        ROUND(CUME_DIST() OVER w_all_by_joined_on_ties, 3) AS joined_on_cume_dist,
        SUM(risk_score) OVER (ORDER BY joined_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_risk_score,
        ROUND(PERCENT_RANK() OVER w_all_by_joined_on_ties, 3) AS joined_on_percent_rank
    FROM deduped
    WINDOW w_region_segment_by_joined_on AS (PARTITION BY region, segment ORDER BY joined_on DESC, id), w_all_by_joined_on_ties AS (ORDER BY joined_on DESC)
),
without_matches AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM sequenced s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.id
              AND p.product IS NOT NULL
        )
)
SELECT id, metric_name, amount
FROM (
    SELECT id, running_risk_score, joined_on_cume_dist
    FROM without_matches
) src
UNPIVOT (
    amount FOR metric_name IN (running_risk_score, joined_on_cume_dist)
) unp
ORDER BY id, metric_name
