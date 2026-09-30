WITH ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft DESC NULLS LAST, id) AS latest_rank
        FROM stores
    ) x
    WHERE latest_rank = 1
)
SELECT
    id,
    store_name,
    city,
    region,
    opened_date,
    latest_rank,
    CASE WHEN region RLIKE '^(West|Central)$' THEN 'GROUP_A' WHEN region RLIKE '^(North|South)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS region_zone
FROM ranked
ORDER BY opened_date DESC, city DESC;
