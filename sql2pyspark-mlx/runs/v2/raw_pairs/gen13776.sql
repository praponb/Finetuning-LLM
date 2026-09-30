SELECT
    id,
    rating,
    country,
    supplier_name,
    COALESCE(rating, 0) rating_filled,
    CASE WHEN country IN ('TH', 'DE') THEN 'Group A' ELSE 'Other' END country_group
FROM suppliers
WHERE city = 'New York' AND rating < 3.6
ORDER BY id DESC
