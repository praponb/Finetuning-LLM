WITH filtered_products AS (
    SELECT *
    FROM products
    WHERE ARRAY_CONTAINS(tags, 'eco')
),
summary AS (
    SELECT category, brand, SORT_ARRAY(COLLECT_SET(brand)) AS brand_list
    FROM filtered_products
    GROUP BY category, brand
)
SELECT category, brand, brand_list
FROM summary;
