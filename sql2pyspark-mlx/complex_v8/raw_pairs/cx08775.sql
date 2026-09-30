WITH prepared_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown
    FROM policies
    WHERE premium >= 2013.8
      AND status IN ('ACTIVE', 'CANCELLED')
),
with_activity AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM prepared_policys s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.product = 'TRAVEL'
        )
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_activity s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
        )
)
SELECT
    COALESCE(product_or_unknown, 'ALL') AS product_or_unknown,
    holder_id,
    GROUPING_ID() AS grouping_level,
    COUNT(*) FILTER (WHERE product = 'HEALTH') AS n_product_health,
    ROUND(STDDEV(premium), 2) AS stddev_premium,
    SUM(CASE WHEN product = 'TRAVEL' THEN 1 ELSE 0 END) AS n_product_travel
FROM screened
GROUP BY CUBE(product_or_unknown, holder_id)
ORDER BY product_or_unknown NULLS LAST, holder_id NULLS LAST
