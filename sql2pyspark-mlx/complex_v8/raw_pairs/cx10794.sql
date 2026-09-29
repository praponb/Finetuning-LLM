WITH prepared_claims AS (
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
        reported_at,
        CASE WHEN claim_type IN ('medical', 'theft') THEN 'group_a' WHEN claim_type = 'fire' THEN 'group_b' ELSE 'other' END AS claim_type_group,
        LENGTH(TRIM(status)) AS status_len,
        DAYOFWEEK(claim_date) AS claim_date_dayofweek
    FROM claims
    WHERE claim_type = 'theft'
),
claim_type_stats AS (
    -- Max approved_amount per claim_type
    SELECT claim_type, MAX(approved_amount) AS max_approved_amount
    FROM prepared_claims
    GROUP BY claim_type
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_approved_amount,
        ROUND(s.approved_amount - g.max_approved_amount, 2) AS approved_amount_minus_max
    FROM prepared_claims s
    JOIN claim_type_stats g ON g.claim_type = s.claim_type
    WHERE s.approved_amount = g.max_approved_amount
),
enriched_policy AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.status,
        s.reported_at,
        s.status_len,
        s.claim_date_dayofweek,
        s.max_approved_amount,
        p.status AS policy_status
    FROM vs_group s
    LEFT JOIN policies p ON p.id = s.policy_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, metric_value
FROM (
    SELECT id, max_approved_amount, claimed_amount
    FROM enriched_policy
) src
UNPIVOT (
    metric_value FOR metric_name IN (max_approved_amount, claimed_amount)
) unp
