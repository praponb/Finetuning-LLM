WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        city,
        region,
        opened_date,
        CONCAT_WS(',', region, store_name) AS region_store_name_key,
        CEIL(sqft) AS sqft_whole
    FROM stores
    WHERE opened_date > DATE '2023-04-10'
)
-- Semi-join via a correlated subquery
SELECT s.*
FROM base s
WHERE EXISTS (
        SELECT 1
        FROM inventory c
        WHERE c.store_id = s.id
          AND c.last_restock >= s.opened_date
    );
