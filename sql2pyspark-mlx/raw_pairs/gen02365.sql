WITH valid_products AS (
    SELECT *
    FROM products
    WHERE brand IN ('Stark', 'Initech')
),
agg AS (
    SELECT category, CASE WHEN brand IN ('Globex', 'Stark') THEN 'group_a' ELSE 'other' END AS brand_group, SORT_ARRAY(COLLECT_SET(category)) AS category_list
    FROM valid_products
    GROUP BY category, CASE WHEN brand IN ('Globex', 'Stark') THEN 'group_a' ELSE 'other' END
)
SELECT category, brand_group, category_list
FROM agg
ORDER BY category, brand_group
LIMIT 5
