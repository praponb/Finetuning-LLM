WITH recent_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date,
        ABS(premium - 3521.8) AS premium_distance
    FROM policies
    WHERE holder_id >= 10
),
with_parent AS (
    -- Join policyholders
    SELECT
        s.*,
        p.holder_name,
        p.segment AS holder_segment,
        p.risk_score AS holder_risk_score
    FROM recent_policys s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
qualified AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM with_parent s
    WHERE s.holder_id IN (
            SELECT p.holder_id
            FROM policies p
            WHERE p.premium <= 3544.1
        )
)
SELECT
    product,
    holder_segment,
    ROUND(SUM(premium), 2) AS total_premium,
    COUNT(DISTINCT product) AS distinct_products
FROM qualified
GROUP BY CUBE(product, holder_segment)
ORDER BY product NULLS LAST, holder_segment NULLS LAST
