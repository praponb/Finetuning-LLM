WITH base_claims AS (
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
        TO_DATE(reported_at) AS reported_at_date,
        CONCAT_WS(' / ', status, claim_type) AS status_claim_type_label,
        DATE_TRUNC('WEEK', reported_at) AS reported_at_week
    FROM claims
    WHERE HOUR(reported_at) BETWEEN 9 AND 13
      AND adjuster_id <= 5
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(claimed_amount - AVG(claimed_amount) OVER w_adjuster_id_claim_type, 2) AS claimed_amount_vs_adjuster_id_claim_type_avg,
        MIN(approved_amount) OVER w_adjuster_id_claim_type AS adjuster_id_claim_type_min_approved_amount,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY adjuster_id, claim_type), 2) AS adjuster_id_claim_type_avg_claimed_amount,
        ROUND(CUME_DIST() OVER (ORDER BY reported_at_date DESC), 3) AS reported_at_date_cume_dist,
        LAST_VALUE(reported_at_date) OVER (ORDER BY reported_at_week DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_reported_at_date,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY adjuster_id, claim_type), 0), 4) AS approved_amount_share_of_adjuster_id_claim_type
    FROM base_claims
    WINDOW w_adjuster_id_claim_type AS (PARTITION BY adjuster_id, claim_type)
),
with_activity AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM windowed s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.adjuster_id IS NOT NULL
              AND c.claim_date IS NOT NULL
        )
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(reported_at_date, 2) OVER (ORDER BY adjuster_id_claim_type_min_approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at_date,
        SIZE(COLLECT_SET(adjuster_id) OVER w_policy_id_status) AS distinct_adjuster_id_in_policy_id_status,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER w_policy_id_status, 0), 4) AS approved_amount_share_of_policy_id_status
    FROM with_activity
    WINDOW w_policy_id_status AS (PARTITION BY policy_id, status)
)
-- Subtotals with CUBE
SELECT
    adjuster_id,
    policy_id,
    GROUPING(adjuster_id) AS is_total_adjuster_id,
    SUM(reported_at_date_cume_dist) FILTER (WHERE status IN ('PAID', 'IN_REVIEW')) AS reported_at_date_cume_dist_status_in_paid_in_rev
FROM metrics
GROUP BY CUBE(adjuster_id, policy_id)
ORDER BY adjuster_id NULLS LAST, policy_id NULLS LAST
