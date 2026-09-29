WITH cleaned AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        ROUND(approved_amount * 1.5, 2) AS approved_amount_adj
    FROM claims
    WHERE HOUR(reported_at) BETWEEN 7 AND 12
      AND approved_amount > 799.5
),
enriched_policy AS (
    -- Attach policy attributes
    SELECT
        s.*,
        p.product AS policy_product,
        p.end_date AS policy_end_date,
        p.start_date AS policy_start_date,
        po.region AS holder_region
    FROM cleaned s
    JOIN policies p ON p.id = s.policy_id
    JOIN policyholders po ON po.id = p.holder_id
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT adjuster_id, COUNT(*) AS claim_count
    FROM claims
    GROUP BY adjuster_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count
    FROM enriched_policy s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
)
SELECT id, metric_name, reading
FROM (
    SELECT id, approved_amount, approved_amount_adj
    FROM with_stats
) src
UNPIVOT (
    reading FOR metric_name IN (approved_amount, approved_amount_adj)
) unp
