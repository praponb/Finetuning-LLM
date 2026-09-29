WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM claims
    WHERE status IN ('APPROVED', 'OPEN')
      AND approved_amount >= 5336.7
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        LEAD(claimed_amount) OVER (PARTITION BY policy_id ORDER BY claim_date, id) AS next_claimed_amount,
        DATEDIFF(claim_date, LAG(claim_date) OVER w_policy_id_by_claim_date) AS days_since_prev_claim_date,
        COUNT(*) OVER (PARTITION BY status) AS rows_in_status,
        SUM(claimed_amount) OVER (PARTITION BY policy_id ORDER BY reported_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_claimed_amount
    FROM recent
    WINDOW w_policy_id_by_claim_date AS (PARTITION BY policy_id ORDER BY claim_date, id)
),
claim_type_stats AS (
    -- Avg days_since_prev_claim_date per claim_type
    SELECT claim_type, AVG(days_since_prev_claim_date) AS avg_days_since_prev_claim_date
    FROM ranked
    GROUP BY claim_type
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_days_since_prev_claim_date,
        ROUND(s.days_since_prev_claim_date - g.avg_days_since_prev_claim_date, 2) AS days_since_prev_claim_date_minus_avg
    FROM ranked s
    JOIN claim_type_stats g ON g.claim_type = s.claim_type
    WHERE s.days_since_prev_claim_date > g.avg_days_since_prev_claim_date
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        rows_in_status - claimed_amount AS rows_in_status_minus_claimed_amount,
        ROUND(running_claimed_amount / NULLIF(next_claimed_amount, 0), 3) AS running_claimed_amount_per_next_claimed_amount,
        FLOOR(running_claimed_amount / 100000) * 100000 AS running_claimed_amount_bucket
    FROM compared
)
-- Final projection
SELECT DISTINCT reported_at, running_claimed_amount_bucket, claim_type
FROM labeled
ORDER BY running_claimed_amount_bucket DESC NULLS FIRST
