WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        CONCAT_WS('-', status, claim_type) AS status_claim_type_label,
        DATEDIFF(DATE '2026-01-01', claim_date) AS days_since_claim_date,
        CONCAT(claim_type, '#', CAST(id AS STRING)) AS claim_type_ref
    FROM claims
    WHERE claimed_amount < 32441.2
),
adjuster_id_stats AS (
    -- Avg approved_amount per adjuster_id
    SELECT adjuster_id, AVG(approved_amount) AS avg_approved_amount
    FROM prepared
    GROUP BY adjuster_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_approved_amount,
        ROUND(s.approved_amount - g.avg_approved_amount, 2) AS approved_amount_minus_avg
    FROM prepared s
    JOIN adjuster_id_stats g ON g.adjuster_id = s.adjuster_id
    WHERE s.approved_amount > g.avg_approved_amount
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, metric_value
FROM (
    SELECT id, claimed_amount, approved_amount_minus_avg, approved_amount, avg_approved_amount
    FROM outliers
) src
UNPIVOT (
    metric_value FOR measure IN (claimed_amount AS claimed_amount_v, approved_amount_minus_avg AS approved_amount_minus_avg_v, approved_amount AS m_approved_amount, avg_approved_amount AS m_avg_approved_amount)
) unp
