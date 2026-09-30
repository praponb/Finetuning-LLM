WITH cleaned_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status,
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        CONCAT_WS('-', status, product) AS status_product_label,
        ROUND(premium * 1.07, 2) AS premium_adj
    FROM policies
    WHERE premium BETWEEN 1042.5 AND 3532.7
      AND status = 'ACTIVE'
),
combined_details_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.premium_bucket,
        p.risk_score AS holder_risk_score,
        p.region AS holder_region
    FROM cleaned_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
latest_per_key AS (
    -- Keep the latest row per holder_region
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        premium_bucket,
        holder_risk_score,
        holder_region
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_region ORDER BY holder_risk_score DESC, id DESC) AS rn
        FROM combined_details_holder
    ) ranked
    WHERE rn = 1
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM latest_per_key
    WHERE holder_risk_score > (SELECT AVG(holder_risk_score) FROM latest_per_key)
)
SELECT
    id,
    holder_id,
    start_date,
    end_date,
    premium_bucket,
    holder_risk_score,
    holder_region
FROM without_matches
ORDER BY holder_region NULLS LAST, premium_bucket DESC, id
LIMIT 10
