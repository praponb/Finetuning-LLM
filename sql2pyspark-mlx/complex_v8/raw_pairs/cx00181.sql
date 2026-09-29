WITH scoped_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        ROUND(premium * 0.25, 2) AS premium_adj,
        FLOOR(premium / 1000) * 1000 AS premium_bucket
    FROM policies
    WHERE holder_id <= 18
      AND product IN ('TRAVEL', 'AUTO', 'HOME')
),
unique_rows AS (
    -- Keep the latest row per product
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        premium_adj,
        premium_bucket
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product ORDER BY premium DESC, id DESC) AS recency_rank
        FROM scoped_policys
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    holder_id,
    product,
    COUNT(*) FILTER (WHERE product IN ('HOME', 'AUTO', 'LIFE')) AS n_product_in_home_auto_life
FROM unique_rows
GROUP BY CUBE(holder_id, product)
ORDER BY holder_id NULLS LAST, product NULLS LAST
