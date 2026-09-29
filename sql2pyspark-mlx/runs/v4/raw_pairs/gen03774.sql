WITH valid_products AS (
    SELECT *, COALESCE(brand, 'Unknown') AS brand_filled
    FROM products
    WHERE ARRAY_CONTAINS(tags, 'premium')
),
agg AS (
    SELECT CASE WHEN cost >= 780 THEN 'high' WHEN cost >= 170 THEN 'mid' ELSE 'low' END AS cost_bucket, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(category))) AS category_values
    FROM valid_products
    GROUP BY CASE WHEN cost >= 780 THEN 'high' WHEN cost >= 170 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM agg
ORDER BY cost_bucket DESC
LIMIT 5;
