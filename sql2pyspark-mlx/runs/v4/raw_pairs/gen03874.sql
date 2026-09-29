WITH kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM stores s
    WHERE EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.store_id = s.id
              AND ch.last_restock <= s.opened_date
        )
)
SELECT
    id,
    store_name,
    region,
    opened_date,
    sqft,
    CASE WHEN region RLIKE '^(West|North)$' THEN 'OTHER_ZONE' WHEN region RLIKE '^(South|East)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS region_zone
FROM kept;
