SELECT
    id,
    supplier_name,
    city,
    rating,
    CASE WHEN country IN ('JP', 'US') THEN 'Group A' ELSE 'Other' END AS country_group
FROM suppliers
WHERE country = 'TH'
ORDER BY 2
