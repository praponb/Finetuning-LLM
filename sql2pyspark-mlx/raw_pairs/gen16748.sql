WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(sqft) OVER (PARTITION BY city) city_avg_sqft,
        PERCENT_RANK() OVER (PARTITION BY city ORDER BY sqft) sqft_percent_rank
    FROM stores
    WHERE city IN ('Bangkok', 'London', 'Berlin')
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.sale_date <= s.opened_date
        )
),
with_latest AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.units
    FROM eligible s
    JOIN LATERAL (
            SELECT c.units
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
eligible_stores AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.order_date < s.opened_date
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible_stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.order_date < s.opened_date
        )
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END is_bangkok,
        DATEDIFF(CURRENT_DATE(), opened_date) days_since_opened_date,
        COALESCE(region, 'UNKNOWN') region_filled
    FROM not_flagged
    WHERE opened_date > DATE '2023-07-08'
      AND city = 'Chiang Mai'
)
SELECT
    id,
    store_name,
    region,
    opened_date,
    sqft,
    city_avg_sqft,
    region_filled
FROM valid
