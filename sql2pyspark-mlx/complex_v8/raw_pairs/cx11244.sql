WITH cleaned AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        ABS(premium - 2033.8) AS premium_distance,
        LENGTH(TRIM(product)) AS product_len
    FROM policies
    WHERE premium BETWEEN 1132.1 AND 4670.9
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.end_date,
        s.status,
        s.premium_distance,
        p.risk_score AS holder_risk_score
    FROM cleaned s
    LEFT JOIN policyholders p ON p.id = s.holder_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, metric_value
FROM (
    SELECT id, holder_risk_score, premium_distance
    FROM enriched
) src
UNPIVOT (
    metric_value FOR measure IN (holder_risk_score, premium_distance)
) unp
ORDER BY id, measure
