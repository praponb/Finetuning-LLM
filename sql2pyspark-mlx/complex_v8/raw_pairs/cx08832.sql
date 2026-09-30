WITH filtered_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date,
        INITCAP(TRIM(product)) AS product_clean,
        SUBSTRING(TRIM(product), 1, 3) AS product_prefix
    FROM policies
    WHERE premium < 3544.1
),
joined_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.days_start_date_to_end_date,
        s.product_clean,
        s.product_prefix,
        p.risk_score AS holder_risk_score,
        p.joined_on AS holder_joined_on,
        p.segment AS holder_segment
    FROM filtered_policys s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.segment = 'SME'
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        LEAST(premium, holder_risk_score) AS least_premium_holder_risk_score,
        LOWER(REGEXP_REPLACE(TRIM(product_clean), ' ', '_')) AS product_clean_slug
    FROM joined_holder
)
SELECT *
FROM (
    SELECT holder_id, holder_segment, premium
    FROM tagged
) src
PIVOT (
    MIN(premium)
    FOR holder_segment IN ('RETAIL', 'SME')
)
ORDER BY holder_id
