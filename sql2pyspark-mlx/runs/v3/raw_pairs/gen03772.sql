SELECT sqft, region
FROM stores
WHERE LENGTH(store_name) >= 11
UNION ALL
SELECT sqft, region
FROM stores
WHERE opened_date > '2024-05-02';
