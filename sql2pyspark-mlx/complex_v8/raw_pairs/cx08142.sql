WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATE_ADD(end_date, 7) AS end_date_plus_7d,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug
    FROM policies
    WHERE holder_id <= 18
      AND premium > 2013.8
),
combined_details AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.end_date_plus_7d,
        s.product_slug,
        p.segment AS holder_segment,
        p.holder_name,
        p.joined_on AS holder_joined_on
    FROM recent s
    LEFT JOIN policyholders p ON p.id = s.holder_id AND p.risk_score >= 0.2
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.product_slug,
        s.holder_segment,
        s.holder_name,
        s.holder_joined_on,
        p.holder_name AS holder_holder_name,
        p.joined_on AS holder_joined_on_2
    FROM combined_details s
    JOIN policyholders p ON p.id = s.holder_id AND LENGTH(TRIM(p.holder_name)) >= 4
)
SELECT
    holder_segment,
    product,
    holder_id,
    COUNT(DISTINCT product) AS distinct_products,
    ROUND(100 * COUNT(CASE WHEN product <> 'HEALTH' THEN 1 END) / COUNT(*), 1) AS pct_product_health
FROM joined
GROUP BY ROLLUP(holder_segment, product, holder_id)
ORDER BY holder_segment NULLS LAST, product NULLS LAST, holder_id NULLS LAST
