WITH base_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        CASE WHEN status IN ('LAPSED', 'ACTIVE') THEN 'group_a' WHEN status = 'CANCELLED' THEN 'group_b' ELSE 'other' END AS status_group,
        DATEDIFF(DATE '2025-06-30', start_date) AS days_since_start_date
    FROM policies
    WHERE start_date IS NOT NULL
      AND status NOT IN ('CANCELLED', 'EXPIRED')
),
joined AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.end_date,
        s.status_group,
        p.joined_on AS holder_joined_on
    FROM base_policys s
    JOIN policyholders p ON p.id = s.holder_id
)
SELECT
    product,
    status_group,
    holder_id,
    SUM(CASE WHEN product IN ('HEALTH', 'LIFE') THEN 1 ELSE 0 END) AS n_product_in_health_life
FROM joined
GROUP BY ROLLUP(product, status_group, holder_id)
ORDER BY product NULLS LAST, status_group NULLS LAST, holder_id NULLS LAST
