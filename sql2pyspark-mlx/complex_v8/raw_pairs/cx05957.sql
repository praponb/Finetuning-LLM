WITH cleaned_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date
    FROM policies
),
with_activity AS (
    -- Keep rows whose holder matches
    SELECT s.*
    FROM cleaned_policys s
    WHERE EXISTS (
            SELECT 1
            FROM policyholders p
            WHERE p.id = s.holder_id
              AND UPPER(TRIM(p.holder_name)) LIKE 'N%'
        )
),
filtered AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM with_activity s
    WHERE s.holder_id IN (
            SELECT p.holder_id
            FROM policies p
            WHERE p.product = 'LIFE'
              AND p.start_date < DATE '2025-07-07'
        )
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        UPPER(TRIM(product)) AS product_clean,
        CONCAT(product, '#', CAST(holder_id AS STRING)) AS product_ref
    FROM filtered
    WHERE premium < 4678.8
),
with_parent AS (
    -- Join policyholders
    SELECT s.*, p.holder_name, p.segment AS holder_segment
    FROM derived s
    JOIN policyholders p ON p.id = s.holder_id
),
screened AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM with_parent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.approved_amount >= 0.0
              AND c.claim_type IN ('fire', 'flood', 'medical')
        )
)
SELECT
    id,
    holder_id,
    product,
    premium,
    start_date,
    product_clean,
    product_ref,
    holder_segment,
    SUBSTRING(TRIM(holder_name), 1, 3) AS holder_name_prefix,
    FLOOR(premium / 1000) * 1000 AS premium_bucket
FROM screened
ORDER BY holder_segment DESC, premium
