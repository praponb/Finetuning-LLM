WITH filtered_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        DATE_ADD(end_date, 365) AS end_date_plus_365d,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
    FROM policies
    WHERE holder_id <= 5
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM filtered_policys
    WHERE premium < (SELECT MAX(premium) FROM filtered_policys)
),
product_stats AS (
    -- Avg premium per product
    SELECT product, AVG(premium) AS avg_premium
    FROM without_matches
    GROUP BY product
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_premium,
        ROUND(s.premium - g.avg_premium, 2) AS premium_minus_avg
    FROM without_matches s
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
        s.end_date,
        s.status,
        s.end_date_plus_365d,
        s.days_start_date_to_end_date,
        s.premium_minus_avg,
        p.holder_name,
        p.region AS holder_region
    FROM vs_group s
    LEFT JOIN policyholders p ON p.id = s.holder_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, metric_value
FROM (
    SELECT id, premium, premium_minus_avg
    FROM with_details_holder
) src
UNPIVOT (
    metric_value FOR measure IN (premium AS m_premium, premium_minus_avg AS premium_minus_avg_v)
) unp
