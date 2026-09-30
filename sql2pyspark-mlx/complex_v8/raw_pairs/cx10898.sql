WITH base_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status,
        ROUND(premium * 1.07, 2) AS premium_adj,
        INITCAP(TRIM(status)) AS status_clean,
        CASE WHEN product IN ('TRAVEL', 'HOME') THEN 'group_a' WHEN product = 'LIFE' THEN 'group_b' ELSE 'other' END AS product_group
    FROM policies
    WHERE premium > 2096.6
),
filtered AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM base_policys s
    WHERE s.holder_id IN (
            SELECT p.holder_id
            FROM policies p
        )
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        product,
        product_group,
        status,
        SUM(premium_adj) AS total_premium_adj,
        COUNT(*) FILTER (WHERE product NOT IN ('HOME', 'TRAVEL')) AS n_product_not_in_home_travel
    FROM filtered
    GROUP BY product, product_group, status
    HAVING MAX(premium_adj) > 1660.0
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY status) AS rows_in_status,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY status)) AS distinct_product_in_status
    FROM stats
)
SELECT product, product_group, measure, reading
FROM (
    SELECT product, product_group, rows_in_status, n_product_not_in_home_travel
    FROM with_windows
) src
UNPIVOT (
    reading FOR measure IN (rows_in_status, n_product_not_in_home_travel)
) unp
