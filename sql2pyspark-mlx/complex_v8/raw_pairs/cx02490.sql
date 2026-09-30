WITH cleaned AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        COALESCE(premium, 0.0) AS premium_filled
    FROM policies
    WHERE start_date >= DATE '2024-07-19'
      AND end_date BETWEEN DATE '2025-12-05' AND DATE '2026-11-25'
),
enriched_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.end_date,
        s.premium_filled,
        p.segment AS holder_segment,
        p.risk_score AS holder_risk_score
    FROM cleaned s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.risk_score >= 0.1
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        product,
        start_date,
        end_date,
        premium_filled,
        holder_segment,
        holder_risk_score,
        CONCAT(product, '#', CAST(id AS STRING)) AS product_ref
    FROM enriched_holder
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown,
        ROUND(premium_filled / NULLIF(holder_risk_score, 0), 3) AS premium_filled_per_holder_risk_score
    FROM derived
)
SELECT
    product_or_unknown,
    holder_segment,
    PERCENTILE_APPROX(holder_risk_score, 0.5) AS median_holder_risk_score
FROM annotated
GROUP BY CUBE(product_or_unknown, holder_segment)
