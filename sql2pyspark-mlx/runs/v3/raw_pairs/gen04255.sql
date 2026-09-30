WITH base AS (
    SELECT *, UPPER(category) AS category_upper
    FROM products
    WHERE stock_qty > 235 AND stock_qty = 342
),
brand_group_summary AS (
    SELECT CASE WHEN brand IN ('Umbrella', 'Initech') THEN 'tier_1' ELSE 'other' END AS brand_group, ROUND(STDDEV(price), 2) AS stddev_price
    FROM base
    GROUP BY CASE WHEN brand IN ('Umbrella', 'Initech') THEN 'tier_1' ELSE 'other' END
)
SELECT brand_group, stddev_price
FROM brand_group_summary
