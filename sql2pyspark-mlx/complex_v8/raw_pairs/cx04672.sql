WITH recent_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM policies
    WHERE start_date >= DATE '2024-06-19'
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.id) AS claim_count
    FROM recent_policys s
),
with_details AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.status_slug,
        p.risk_score AS holder_risk_score
    FROM with_counts s
    JOIN policyholders p ON p.id = s.holder_id
),
with_parent_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.holder_risk_score,
        p.risk_score AS holder_risk_score_2,
        p.holder_name
    FROM with_details s
    JOIN policyholders p ON p.id = s.holder_id
)
-- Final projection
SELECT
    id,
    premium,
    start_date,
    holder_risk_score,
    holder_risk_score_2,
    holder_name,
    SUBSTRING(TRIM(holder_name), 1, 3) AS holder_name_prefix
FROM with_parent_holder
ORDER BY holder_risk_score_2 NULLS LAST, premium
