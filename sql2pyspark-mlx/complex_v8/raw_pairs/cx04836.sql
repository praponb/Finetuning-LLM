WITH recent AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        risk_score,
        joined_on,
        CONCAT(holder_name, '#', CAST(id AS STRING)) AS holder_name_ref,
        ABS(risk_score - 0.6) AS risk_score_distance,
        LENGTH(TRIM(region)) AS region_len
    FROM policyholders
    WHERE risk_score BETWEEN 0.1 AND 0.6
),
summarized_rows AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM policies p WHERE p.holder_id = s.id) AS policy_count,
        (SELECT COUNT(CASE WHEN p.product = 'HEALTH' THEN 1 END) FROM policies p WHERE p.holder_id = s.id) AS policy_product_count
    FROM recent s
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(risk_score_distance * 0.25, 2) AS risk_score_distance_adj,
        GREATEST(risk_score_distance, policy_product_count) AS greatest_risk_score_distance_policy_product_coun,
        UPPER(TRIM(holder_name_ref)) AS holder_name_ref_clean
    FROM summarized_rows
    WHERE policy_product_count >= 4
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(holder_name_ref), ' ', '_')) AS holder_name_ref_slug,
        DATE_TRUNC('WEEK', joined_on) AS joined_on_week
    FROM scored
    WHERE risk_score_distance < 0.7
)
SELECT
    id,
    holder_name,
    risk_score,
    holder_name_ref,
    risk_score_distance,
    policy_count,
    risk_score_distance_adj,
    greatest_risk_score_distance_policy_product_coun,
    holder_name_ref_clean,
    holder_name_ref_slug,
    joined_on_week,
    LOWER(REGEXP_REPLACE(TRIM(holder_name_ref), ' ', '_')) AS holder_name_ref_slug_2
FROM enriched
WHERE holder_name_ref IS NOT NULL
ORDER BY risk_score_distance_adj, id
LIMIT 20
