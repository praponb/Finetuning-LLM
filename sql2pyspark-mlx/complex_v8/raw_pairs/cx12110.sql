WITH filtered_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug,
        DATE_FORMAT(end_date, 'yyyy-MM-dd') AS end_date_label,
        COALESCE(premium, 0.0) AS premium_filled
    FROM policies
    WHERE status = 'EXPIRED'
      AND premium BETWEEN 1541.8 AND 3521.8
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        product_slug,
        end_date_label,
        premium_filled,
        MONTH(end_date) AS end_date_month,
        SUBSTRING(TRIM(product_slug), 1, 4) AS product_slug_prefix,
        premium_filled + premium AS premium_filled_plus_premium
    FROM filtered_policys
),
enriched_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.end_date,
        s.product_slug,
        s.end_date_label,
        s.premium_filled,
        s.end_date_month,
        s.premium_filled_plus_premium,
        p.holder_name,
        p.risk_score AS holder_risk_score
    FROM labeled s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.risk_score >= 0.2
)
-- Final projection
SELECT
    id,
    product,
    end_date,
    end_date_label,
    end_date_month,
    premium_filled_plus_premium,
    holder_name,
    holder_risk_score
FROM enriched_holder
ORDER BY holder_risk_score DESC, id
LIMIT 10
