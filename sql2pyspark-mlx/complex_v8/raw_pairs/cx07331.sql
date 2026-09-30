WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_30d,
        SUM(b.approved_amount) AS prior_approved_amount_30d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 30)
    GROUP BY a.id, a.policy_id, a.claim_date
),
with_details_policy AS (
    -- Attach policy attributes
    SELECT s.*, p.end_date AS policy_end_date
    FROM covered s
    JOIN policies p ON p.id = s.policy_id
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        policy_end_date,
        DATE_FORMAT(claim_date, 'yyyy-MM-dd') AS claim_date_label,
        MONTH(policy_end_date) AS policy_end_date_month
    FROM with_details_policy
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        COUNT(*) AS claim_count,
        MAX(reported_at) AS last_claim_reported_at
    FROM claims
    GROUP BY policy_id
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count,
        c.last_claim_reported_at
    FROM scored s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
)
SELECT
    id,
    policy_id,
    policy_end_date,
    policy_end_date_month,
    claim_count,
    last_claim_reported_at,
    LENGTH(TRIM(claim_date_label)) AS claim_date_label_len
FROM profiled
ORDER BY policy_end_date_month
