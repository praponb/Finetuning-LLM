WITH valid AS (
    SELECT
        id,
        store_name,
        city,
        region,
        COALESCE(store_name, 'N/A') AS store_name_filled
    FROM stores
    WHERE city = 'Bangkok'
),
cleaned AS (
    SELECT
        *,
        UPPER(region) AS region_upper,
        SUBSTRING(store_name, 1, 3) AS store_name_prefix,
        COALESCE(region, 'UNKNOWN') AS resolved_region
    FROM valid
    WHERE city NOT IN ('New York', 'Tokyo')
),
not_flagged AS (
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.store_id = s.id
              AND ch.discount <= 0.35
        )
)
SELECT
    city,
    COUNT_IF(region = 'West') AS west_events,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM not_flagged
GROUP BY city
HAVING COUNT_IF(region = 'West') >= 1;
