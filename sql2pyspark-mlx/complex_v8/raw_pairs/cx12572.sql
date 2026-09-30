WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        CASE WHEN claim_type IN ('delay', 'flood') THEN 'group_a' WHEN claim_type = 'collision' THEN 'group_b' ELSE 'other' END AS claim_type_group,
        TO_DATE(reported_at) AS reported_at_date
    FROM claims
    WHERE reported_at < TIMESTAMP '2025-02-12 12:00:00'
      AND adjuster_id >= 7
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        reported_at,
        claim_type_group,
        reported_at_date,
        SUBSTRING(TRIM(status), 1, 3) AS status_prefix,
        DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_label,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled
    FROM recent
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MAX(c.reported_at) FROM claims c WHERE c.policy_id = s.policy_id) AS last_claim_reported_at,
        (SELECT ROUND(AVG(c.approved_amount), 2) FROM claims c WHERE c.policy_id = s.policy_id) AS avg_claim_approved_amount
    FROM annotated s
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.reported_at,
        s.claim_type_group,
        s.reported_at_date,
        s.claim_date_label,
        s.last_claim_reported_at,
        s.avg_claim_approved_amount,
        p.end_date AS policy_end_date,
        p.start_date AS policy_start_date
    FROM profiled s
    JOIN policies p ON p.id = s.policy_id
)
-- Subtotals with GROUPING SETS
SELECT
    claim_type_group,
    policy_id,
    adjuster_id,
    GROUPING(claim_type_group) AS is_total_claim_type_group,
    COUNT(DISTINCT id) AS distinct_ids
FROM with_parent
GROUP BY GROUPING SETS ((claim_type_group, policy_id, adjuster_id), (claim_type_group), (policy_id))
ORDER BY claim_type_group NULLS LAST, policy_id NULLS LAST, adjuster_id NULLS LAST
