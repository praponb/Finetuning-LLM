WITH filtered_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        ABS(premium - 2319.8) AS premium_distance,
        UPPER(TRIM(product)) AS product_clean
    FROM policies
    WHERE (status = 'LAPSED' OR start_date < DATE '2025-03-21')
),
joined_holder AS (
    -- Enrich with dimension data
    SELECT s.*, p.segment AS holder_segment
    FROM filtered_policys s
    LEFT JOIN policyholders p ON p.id = s.holder_id AND UPPER(TRIM(p.holder_name)) LIKE 'N%'
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        holder_id,
        ROUND(SUM(premium_distance) / NULLIF(SUM(premium), 0), 3) AS premium_distance_to_premium_ratio,
        ROUND(SUM(premium) / NULLIF(SUM(premium_distance), 0), 3) AS premium_to_premium_distance_ratio,
        SUM(premium) AS total_premium
    FROM joined_holder
    GROUP BY holder_id
    HAVING AVG(premium) > 1541.8
)
-- Native SQL UNPIVOT (wide to long)
SELECT holder_id, metric_name, amount
FROM (
    SELECT holder_id, premium_distance_to_premium_ratio, premium_to_premium_distance_ratio
    FROM stats
) src
UNPIVOT (
    amount FOR metric_name IN (premium_distance_to_premium_ratio AS m_premium_distance_to_premium_ratio, premium_to_premium_distance_ratio AS premium_to_premium_distance_ratio_v)
) unp
