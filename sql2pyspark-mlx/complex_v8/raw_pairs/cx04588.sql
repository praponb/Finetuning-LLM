WITH filtered_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        ABS(claimed_amount - 19347.2) AS claimed_amount_distance
    FROM claims
    WHERE claim_type = 'fire'
      AND approved_amount >= 0.0
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.status,
        a.level AS adjuster_level,
        a.adjuster_name,
        a.team AS adjuster_team
    FROM filtered_claims s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(adjuster_level) OVER (PARTITION BY adjuster_level) AS adjuster_level_min_adjuster_level,
        COUNT(*) OVER (PARTITION BY status, YEAR(claim_date) ORDER BY adjuster_level ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(adjuster_level - AVG(adjuster_level) OVER (PARTITION BY adjuster_level), 2) AS adjuster_level_vs_adjuster_level_avg
    FROM combined_details
)
SELECT id, metric, value
FROM (
    SELECT id, running_count, adjuster_level, adjuster_level_min_adjuster_level
    FROM ranked
) src
UNPIVOT (
    value FOR metric IN (running_count, adjuster_level, adjuster_level_min_adjuster_level)
) unp
