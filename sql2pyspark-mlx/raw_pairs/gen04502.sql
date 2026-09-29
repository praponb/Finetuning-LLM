WITH valid AS (
    SELECT
        id,
        store_name,
        opened_date,
        sqft,
        CONCAT_WS('-', region, city) region_city_key
    FROM stores
    WHERE opened_date BETWEEN '2023-10-22' AND '2023-12-01'
)
SELECT s.*
FROM valid s
WHERE NOT EXISTS (
        SELECT 1
        FROM orders x
        WHERE x.store_id = s.id
          AND x.status = 'PENDING'
    );
