WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, UPPER(store_name) store_name_upper
    FROM stores
    WHERE region IS NOT NULL
      AND region IS DISTINCT FROM 'South'
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.store_id IN (1, 6)
        )
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        city,
        opened_date,
        sqft,
        store_name_upper,
        CASE WHEN sqft >= 13319 THEN 'High' WHEN sqft >= 6314 THEN 'Medium' ELSE 'Low' END sqft_tier
    FROM eligible
    WHERE opened_date > TO_DATE('2023-04-09')
),
latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.city,
        s.opened_date,
        s.sqft,
        s.store_name_upper,
        x.store_sale_units,
        x.revenue
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.units AS store_sale_units, c.revenue
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    store_name,
    opened_date,
    sqft,
    store_name_upper,
    store_sale_units,
    revenue
FROM latest_match;
