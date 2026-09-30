WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        CONCAT(region, ', ', store_name) AS region_store_name
    FROM stores
    WHERE opened_date > DATE '2023-09-27'
      AND city IN ('Tokyo', 'Bangkok')
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN sqft >= 17824 THEN 'High' WHEN sqft >= 5637 THEN 'Medium' ELSE 'Low' END AS sqft_band,
        LENGTH(city) AS city_length
    FROM valid
    WHERE opened_date > DATE '2023-06-24'
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.store_id = s.id
              AND ch.order_date >= s.opened_date
        )
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM kept
    INTERSECT
    SELECT id
    FROM stores
    WHERE city NOT IN ('Singapore', 'London', 'New York')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept s
    JOIN remaining_keys k ON s.id = k.id
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        region,
        sqft,
        region_store_name,
        sqft_band,
        CUME_DIST() OVER (ORDER BY city_length DESC) AS city_length_cume_dist,
        SUM(city_length) OVER (PARTITION BY region) AS region_sum_city_length
    FROM verified
)
SELECT
    id,
    sqft,
    region_store_name,
    city_length_cume_dist,
    region_sum_city_length
FROM enriched
ORDER BY city_length_cume_dist DESC, id;
