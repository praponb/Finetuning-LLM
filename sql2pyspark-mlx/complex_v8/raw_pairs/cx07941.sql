WITH in_window AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.status,
        c.claim_type,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE c.claim_type <> 'medical') FROM claims c WHERE c.policy_id = s.policy_id) AS claim_claim_type_count,
        (SELECT MAX(c.claimed_amount) FROM claims c WHERE c.policy_id = s.policy_id) AS max_claim_claimed_amount,
        (SELECT ROUND(AVG(c.claimed_amount), 2) FROM claims c WHERE c.policy_id = s.policy_id) AS avg_claim_claimed_amount
    FROM in_window s
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        DATE_TRUNC('MONTH', claim_date) AS claim_date_month,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM with_stats
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.approved_amount >= 2887.3
        )
)
SELECT *
FROM (
    SELECT claim_date, product, avg_claim_claimed_amount
    FROM qualified
) src
PIVOT (
    MIN(avg_claim_claimed_amount)
    FOR product IN ('HOME', 'AUTO', 'TRAVEL', 'HEALTH')
)
