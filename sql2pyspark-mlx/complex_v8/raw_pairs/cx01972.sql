WITH cleaned_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        CONCAT(product, '#', CAST(holder_id AS STRING)) AS product_ref,
        CASE WHEN premium >= 4417.0 THEN 'HIGH' WHEN premium >= 2013.8 THEN 'MEDIUM' ELSE 'LOW' END AS premium_tier
    FROM policies
    WHERE premium >= 2111.3
),
enriched AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.status,
        s.premium_tier,
        p.holder_name
    FROM cleaned_policys s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
enriched_2 AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN premium_tier IN ('MEDIUM', 'LOW') THEN 'group_a' WHEN premium_tier = 'HIGH' THEN 'group_b' ELSE 'other' END AS premium_tier_group,
        INITCAP(TRIM(premium_tier)) AS premium_tier_clean
    FROM enriched
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        SUBSTRING(TRIM(product), 1, 3) AS product_prefix,
        LENGTH(TRIM(holder_name)) AS holder_name_len,
        CASE WHEN product IN ('HEALTH', 'AUTO') THEN 'group_a' WHEN product = 'LIFE' THEN 'group_b' ELSE 'other' END AS product_group
    FROM enriched_2
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT product_group, product, premium_tier_group, holder_name_len
    FROM tagged
) src
PIVOT (
    MAX(holder_name_len)
    FOR premium_tier_group IN ('other', 'group_b')
)
