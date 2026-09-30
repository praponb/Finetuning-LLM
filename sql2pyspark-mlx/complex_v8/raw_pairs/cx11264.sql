WITH prepared_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date
    FROM policies
    WHERE end_date BETWEEN DATE '2024-11-08' AND DATE '2027-03-20'
      AND premium BETWEEN 1042.5 AND 3719.7
),
filtered AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM prepared_policys s
    WHERE s.holder_id IN (
            SELECT p.holder_id
            FROM policies p
            WHERE p.start_date BETWEEN DATE '2024-07-19' AND DATE '2025-05-02'
        )
),
product_stats AS (
    -- Avg premium per product
    SELECT product, AVG(premium) AS avg_premium
    FROM filtered
    GROUP BY product
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_premium,
        ROUND(s.premium - g.avg_premium, 2) AS premium_minus_avg
    FROM filtered s
    JOIN product_stats g ON g.product = s.product
    WHERE s.premium > g.avg_premium
),
with_details_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        s.avg_premium,
        p.holder_name,
        p.risk_score AS holder_risk_score,
        p.region AS holder_region
    FROM vs_group s
    JOIN policyholders p ON p.id = s.holder_id
)
SELECT
    id,
    holder_id,
    premium,
    start_date,
    end_date,
    avg_premium,
    holder_name,
    holder_risk_score,
    ABS(premium - 3521.8) AS premium_distance
FROM with_details_holder
WHERE avg_premium < 4280.0
ORDER BY avg_premium DESC, id DESC
