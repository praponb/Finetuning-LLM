WITH valid_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        LAST_DAY(end_date) AS end_date_month_end,
        CASE WHEN premium >= 3465.4 THEN 'A' WHEN premium >= 2033.8 THEN 'B' ELSE 'C' END AS premium_tier
    FROM policies
    WHERE product = 'LIFE'
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        CONCAT_WS('-', premium_tier, status) AS premium_tier_status_label
    FROM valid_policys
),
enriched_holder AS (
    -- Attach holder attributes
    SELECT
        s.*,
        p.region AS holder_region,
        p.risk_score AS holder_risk_score,
        p.joined_on AS holder_joined_on
    FROM tagged s
    JOIN policyholders p ON p.id = s.holder_id AND UPPER(TRIM(p.holder_name)) LIKE 'D%'
    WHERE p.region IN ('East', 'South')
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN c.status = 'APPROVED' THEN 1 END) FROM claims c WHERE c.policy_id = s.id) AS claim_status_count,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.id) AS claim_count
    FROM enriched_holder s
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    start_date,
    end_date,
    status,
    premium_tier,
    status_slug,
    premium_tier_status_label,
    holder_region,
    holder_risk_score,
    holder_joined_on,
    claim_status_count,
    claim_count
FROM with_counts
ORDER BY id
