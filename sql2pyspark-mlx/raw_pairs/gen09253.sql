WITH not_flagged AS (
    SELECT s.*
    FROM stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND ch.sale_date >= s.opened_date
        )
),
cleaned AS (
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        FLOOR(sqft) sqft_whole,
        COALESCE(region, 'OTHER') resolved_region
    FROM not_flagged
    WHERE city = 'Chiang Mai'
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft ASC, id ASC) AS rn
        FROM cleaned
    ) ranked
    WHERE rn = 1
),
latest AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY city, region ORDER BY sqft_whole DESC) AS rn_2
        FROM ranked
    ) t
    WHERE rn_2 = 1
)
SELECT
    id,
    city,
    opened_date,
    sqft,
    sqft_whole,
    resolved_region,
    rn_2
FROM latest;
