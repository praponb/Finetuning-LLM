WITH base AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        ABS(claimed_amount - 29359.4) AS claimed_amount_distance
    FROM claims
    WHERE claim_date >= DATE '2024-04-15'
      AND policy_id <= 5
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(3) OVER (ORDER BY reported_at DESC, id) AS reported_at_tercile,
        ROUND(CUME_DIST() OVER w_reported_at_year_by_approved, 3) AS approved_amount_cume_dist,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY adjuster_id, policy_id), 0), 4) AS approved_amount_share_of_adjuster_id_policy_id,
        claimed_amount - LAG(claimed_amount) OVER (ORDER BY reported_at DESC, id) AS claimed_amount_change
    FROM base
    WINDOW w_reported_at_year_by_approved AS (PARTITION BY YEAR(reported_at) ORDER BY approved_amount, id)
)
SELECT
    COALESCE(claim_type, 'ALL') AS claim_type,
    adjuster_id,
    GROUPING(claim_type) AS is_total_claim_type,
    MAX(approved_amount_share_of_adjuster_id_policy_id) AS max_approved_amount_share_of_adjuster_id_policy_,
    ROUND(AVG(approved_amount), 2) AS avg_approved_amount
FROM with_trends
GROUP BY CUBE(claim_type, adjuster_id)
ORDER BY claim_type NULLS LAST, adjuster_id NULLS LAST
