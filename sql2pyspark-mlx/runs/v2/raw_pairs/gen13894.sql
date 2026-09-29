SELECT
    id,
    sqft,
    city,
    store_name,
    COALESCE(sqft, 0) AS sqft_filled,
    CASE WHEN region IN ('North', 'Central') THEN 'Group A' ELSE 'Other' END AS region_group
FROM stores
WHERE LOWER(store_name) LIKE '%ive%' AND sqft >= 14442 AND opened_date BETWEEN '2023-12-01' AND '2024-02-01'
ORDER BY city ASC, id ASC
LIMIT 10
