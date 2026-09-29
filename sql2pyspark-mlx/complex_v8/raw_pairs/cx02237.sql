WITH valid_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        SUBSTRING(TRIM(claim_type), 1, 2) AS claim_type_prefix
    FROM claims
    WHERE claimed_amount < 40638.4
      AND claim_date >= DATE '2024-08-10'
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MAX(c.reported_at) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS last_claim_reported_at,
        (SELECT ROUND(AVG(c.approved_amount), 2) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS avg_claim_approved_amount,
        (SELECT MAX(c.claim_date) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS last_claim_date
    FROM valid_claims s
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        claim_type_prefix,
        last_claim_reported_at,
        avg_claim_approved_amount,
        last_claim_date,
        SIZE(COLLECT_SET(claim_type) OVER w_policy_id) AS distinct_claim_type_in_policy_id,
        MAX(claimed_amount) OVER (PARTITION BY claim_type ORDER BY last_claim_reported_at ASC NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS claimed_amount_max_7,
        RANK() OVER (PARTITION BY claim_type ORDER BY last_claim_reported_at ASC NULLS LAST, id) AS last_claim_reported_at_rank,
        ROUND(approved_amount - AVG(approved_amount) OVER w_policy_id, 2) AS approved_amount_vs_policy_id_avg
    FROM with_stats
    WINDOW w_policy_id AS (PARTITION BY policy_id)
)
-- Subtotals with GROUPING SETS
SELECT
    adjuster_id,
    distinct_claim_type_in_policy_id,
    policy_id,
    GROUPING_ID() AS grouping_level,
    MAX(claimed_amount) AS max_claimed_amount,
    DATEDIFF(MAX(last_claim_date), MIN(last_claim_date)) AS last_claim_date_span_days
FROM sequenced
GROUP BY GROUPING SETS ((adjuster_id, distinct_claim_type_in_policy_id, policy_id), (adjuster_id), (distinct_claim_type_in_policy_id))
ORDER BY adjuster_id NULLS LAST, distinct_claim_type_in_policy_id NULLS LAST, policy_id NULLS LAST
