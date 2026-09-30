WITH not_flagged AS (
    SELECT s.*
    FROM stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.store_id IN (1, 2, 3, 5)
        )
)
SELECT
    id,
    city,
    region,
    sqft,
    CASE WHEN city RLIKE '^(New York|Tokyo)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS city_zone
FROM not_flagged;
