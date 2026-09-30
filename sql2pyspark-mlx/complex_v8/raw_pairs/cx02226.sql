WITH valid_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        ABS(premium - 2844.1) AS premium_distance,
        COALESCE(premium, 0.0) AS premium_filled
    FROM policies
    WHERE start_date IS NOT NULL
),
combined_details_holder AS (
    -- Join policyholders
    SELECT
        s.*,
        p.holder_name,
        p.risk_score AS holder_risk_score,
        p.joined_on AS holder_joined_on
    FROM valid_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
with_parent_holder AS (
    -- Attach holder attributes
    SELECT s.*, p.region AS holder_region
    FROM combined_details_holder s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
screened AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM with_parent_holder s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.status IS NOT NULL
        )
)
SELECT
    id,
    product,
    premium,
    start_date,
    end_date,
    status,
    premium_distance,
    holder_name,
    holder_risk_score,
    holder_joined_on,
    holder_region,
    premium_filled + premium_distance AS premium_filled_plus_premium_distance,
    SUBSTRING(TRIM(holder_region), 1, 3) AS holder_region_prefix
FROM screened
ORDER BY product DESC NULLS FIRST, id
LIMIT 20
