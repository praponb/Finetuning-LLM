SELECT id, store_name, LOWER(region) AS region_lower
FROM stores
WHERE NOT (city <> 'London' AND region IN ('North', 'East'));
