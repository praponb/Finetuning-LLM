WITH filtered_products AS (
    SELECT *, INITCAP(TRIM(product_name)) AS product_name_title
    FROM products
    WHERE launch_date >= '2025-05-01' OR SIZE(tags) > 1
),
agg AS (
    SELECT CASE WHEN category IN ('Sports', 'Home') THEN 'tier_1' ELSE 'other' END AS category_group, SORT_ARRAY(COLLECT_SET(brand)) AS brand_list
    FROM filtered_products
    GROUP BY CASE WHEN category IN ('Sports', 'Home') THEN 'tier_1' ELSE 'other' END
)
SELECT *
FROM agg
ORDER BY category_group;
