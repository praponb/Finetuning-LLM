WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        cost * price AS margin,
        COALESCE(brand, 'N/A') AS brand_filled
    FROM products
    WHERE launch_date > DATE '2023-06-03'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(price) OVER (PARTITION BY category ORDER BY margin, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_price,
        MIN(price) OVER (PARTITION BY category ORDER BY margin, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_price,
        SUM(price) OVER (PARTITION BY category ORDER BY margin, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_price
    FROM filtered
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY running_count_price DESC NULLS LAST, id) AS latest_rank
    FROM with_windows
    QUALIFY latest_rank = 1
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        product_name,
        brand,
        stock_qty,
        tags,
        supplier_id,
        margin,
        brand_filled,
        running_count_price,
        running_sum_price,
        latest_rank
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY brand, supplier_id ORDER BY latest_rank, id) AS rn
        FROM deduped
    ) x
    WHERE rn = 1
)
SELECT
    id,
    brand,
    stock_qty,
    brand_filled,
    running_count_price,
    latest_rank,
    CASE WHEN brand RLIKE '^(Globex|Initech)$' THEN 'OTHER_ZONE' WHEN brand RLIKE '^(Acme|Umbrella)$' THEN 'GROUP_B' ELSE 'CORE' END AS brand_zone
FROM latest
ORDER BY brand_zone DESC, stock_qty DESC;
