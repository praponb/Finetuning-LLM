WITH base_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        TRUNC(end_date, 'MM') AS end_date_month_start,
        CONCAT(status, '#', CAST(holder_id AS STRING)) AS status_ref,
        CASE WHEN premium >= 3817.5 THEN 'large' WHEN premium >= 1132.1 THEN 'medium' ELSE 'small' END AS premium_tier
    FROM policies
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM base_policys s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.claim_date < DATE '2024-12-05'
              AND c.approved_amount > 0.0
        )
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        SUM(approved_amount) AS total_claim_approved_amount,
        MAX(reported_at) AS last_claim_reported_at
    FROM claims
    GROUP BY policy_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.total_claim_approved_amount,
        c.last_claim_reported_at
    FROM qualified s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
)
SELECT id, metric, value
FROM (
    SELECT id, total_claim_approved_amount, premium
    FROM with_counts
) src
UNPIVOT (
    value FOR metric IN (total_claim_approved_amount, premium)
) unp
ORDER BY id, metric
