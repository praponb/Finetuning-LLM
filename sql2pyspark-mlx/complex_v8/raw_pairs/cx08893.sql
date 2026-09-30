WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        CASE WHEN status IN ('PAID', 'IN_REVIEW') THEN 'group_a' WHEN status = 'OPEN' THEN 'group_b' ELSE 'other' END AS status_group,
        INITCAP(TRIM(claim_type)) AS claim_type_clean,
        CONCAT_WS('-', status, claim_type) AS status_claim_type_label
    FROM claims
),
summary AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', claim_date) AS claim_date_month,
        policy_id,
        MIN(claim_date) AS first_claim_date,
        MAX(approved_amount) AS max_approved_amount,
        DATEDIFF(MAX(claim_date), MIN(claim_date)) AS claim_date_span_days,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) FILTER (WHERE claim_type = 'medical') AS n_claim_type_medical
    FROM prepared
    WHERE approved_amount <= 32511.8
    GROUP BY DATE_TRUNC('MONTH', claim_date), policy_id
    HAVING MAX(claimed_amount) > 4234.9
),
with_parent AS (
    -- Join policies
    SELECT
        s.claim_date_month,
        s.policy_id,
        s.max_approved_amount,
        s.claim_date_span_days,
        s.distinct_ids,
        p.status AS policy_status
    FROM summary s
    JOIN policies p ON p.id = s.policy_id
),
filtered AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_parent s
    WHERE s.policy_id IN (
            SELECT c.policy_id
            FROM claims c
            WHERE c.claim_type <> 'medical'
              AND c.claimed_amount > 13898.1
        )
      AND s.claim_date_month >= TIMESTAMP '2025-01-25 09:00:00'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(max_approved_amount, 3) OVER (ORDER BY max_approved_amount DESC, claim_date_month, policy_id) AS prev3_max_approved_amount,
        claim_date_span_days - LAG(claim_date_span_days) OVER (ORDER BY max_approved_amount DESC, claim_date_month, policy_id) AS claim_date_span_days_change,
        LAST_VALUE(claim_date_month) OVER (ORDER BY max_approved_amount DESC, claim_date_month, policy_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claim_date_month,
        FIRST_VALUE(max_approved_amount) OVER (ORDER BY distinct_ids DESC, claim_date_month, policy_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_max_approved_amount
    FROM filtered
)
-- Final projection
SELECT
    claim_date_month,
    policy_id,
    max_approved_amount,
    claim_date_span_days,
    distinct_ids,
    policy_status,
    prev3_max_approved_amount,
    last_claim_date_month,
    first_max_approved_amount,
    GREATEST(distinct_ids, max_approved_amount) AS greatest_distinct_ids_max_approved_amount
FROM windowed
ORDER BY distinct_ids
