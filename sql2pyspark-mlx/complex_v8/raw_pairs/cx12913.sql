WITH valid_claims AS (
    -- Scope the claims rows
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
        ROUND(claimed_amount * 1.07, 2) AS claimed_amount_adj,
        DATE_FORMAT(claim_date, 'MMM yyyy') AS claim_date_label,
        CASE WHEN claimed_amount >= 42447.2 THEN 'large' WHEN claimed_amount >= 20436.5 THEN 'medium' ELSE 'small' END AS claimed_amount_tier
    FROM claims
    WHERE approved_amount <= 17759.3
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count_2
    FROM valid_claims s
),
claim_stats_2 AS (
    -- Per-adjuster claims metrics
    SELECT
        adjuster_id,
        COUNT(*) AS claim_count_3,
        COUNT(*) FILTER (WHERE status IN ('OPEN', 'PAID')) AS claim_status_count,
        COUNT(*) AS claim_count_4
    FROM claims
    GROUP BY adjuster_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count_3, 0) AS claim_count_3,
        COALESCE(c.claim_status_count, 0) AS claim_status_count,
        COALESCE(c.claim_count_4, 0) AS claim_count_4
    FROM summarized_rows s
    LEFT JOIN claim_stats_2 c ON c.adjuster_id = s.adjuster_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        status,
        reported_at,
        claimed_amount_adj,
        claim_count,
        claim_count_2,
        claim_status_count,
        claim_count_4,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY claim_count, claimed_amount_tier)) AS distinct_claim_type_in_claim_count_claimed_amoun,
        ROUND(AVG(claim_count_2) OVER (ORDER BY claim_count_2 ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS claim_count_2_avg_3
    FROM with_counts
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT policy_id, reported_at, status, claim_count_2_avg_3
    FROM metrics
) src
PIVOT (
    SUM(claim_count_2_avg_3)
    FOR status IN ('REJECTED', 'OPEN', 'PAID', 'IN_REVIEW')
)
