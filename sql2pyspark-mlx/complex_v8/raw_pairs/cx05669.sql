WITH covered AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.claim_type,
        c.approved_amount,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY policy_id) AS rows_in_policy_id,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY claim_type ORDER BY premium, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS claimed_amount_avg_2,
        ROUND(CUME_DIST() OVER w_policy_id_claim_type_by_appr, 3) AS approved_amount_cume_dist,
        RANK() OVER w_policy_id_claim_type_by_appr AS approved_amount_rank,
        ROW_NUMBER() OVER (PARTITION BY policy_id, claim_type ORDER BY approved_amount, id) AS rn
    FROM covered
    WINDOW w_policy_id_claim_type_by_appr AS (PARTITION BY policy_id, claim_type ORDER BY approved_amount)
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_trends
    WHERE premium > (SELECT AVG(premium) FROM with_trends)
),
line_level_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.claimed_amount,
        s.claim_type,
        s.approved_amount,
        s.product,
        s.premium,
        s.claimed_amount_avg_2,
        s.approved_amount_rank,
        c.id AS claim_id,
        c.status AS claim_status,
        c.reported_at AS claim_reported_at,
        c.approved_amount AS claim_approved_amount
    FROM qualified s
    JOIN claims c ON c.policy_id = s.policy_id
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    claimed_amount,
    claim_type,
    premium,
    approved_amount_rank,
    claim_id,
    claim_status,
    DATE_FORMAT(claim_date, 'yyyy-MM-dd') AS claim_date_label,
    CASE WHEN approved_amount_rank >= 7 THEN 'A' WHEN approved_amount_rank >= 4 THEN 'B' ELSE 'C' END AS approved_amount_rank_tier
FROM line_level_claims
WHERE approved_amount <= 16404.6
