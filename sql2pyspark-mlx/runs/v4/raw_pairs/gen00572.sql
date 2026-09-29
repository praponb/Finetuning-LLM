WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        city,
        region,
        opened_date,
        COALESCE(sqft, 0) AS sqft_filled
    FROM stores
    WHERE city NOT IN ('Chiang Mai', 'London')
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.inventory_last_restock
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(sqft_filled) OVER (PARTITION BY city ORDER BY sqft_filled, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS sqft_filled_avg_5_prior,
        FIRST_VALUE(sqft_filled) IGNORE NULLS OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sqft_filled
    FROM with_latest
),
with_last AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.last_restock,
        latest.inventory_quantity
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.last_restock, c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        city,
        region,
        first_known_sqft_filled,
        last_restock,
        inventory_quantity,
        FLOOR(first_known_sqft_filled) AS first_known_sqft_filled_whole,
        DATEDIFF(opened_date, inventory_last_restock) AS days_inventory_last_restock_to_opened_date
    FROM with_last
    WHERE MONTH(inventory_last_restock) IN (3, 4, 7)
),
with_windows_stores AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(first_known_sqft_filled_whole) OVER (ORDER BY days_inventory_last_restock_to_opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_first_known_sqft_filled_whole,
        NTILE(10) OVER (ORDER BY first_known_sqft_filled, id) AS first_known_sqft_filled_bucket,
        NTILE(5) OVER (PARTITION BY region ORDER BY days_inventory_last_restock_to_opened_date DESC, id) AS days_inventory_last_restock_to_opened_date_bucket
    FROM valid
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM with_windows_stores
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY last_restock DESC NULLS LAST, id) = 1
),
daily AS (
    -- Grouped metrics
    SELECT
        CASE WHEN first_known_sqft_filled_bucket >= 9 THEN 'HIGH' ELSE 'LOW' END AS first_known_sqft_filled_bucket_tier,
        city,
        CASE WHEN running_first_known_sqft_filled_whole >= 354 THEN 'HIGH' ELSE 'LOW' END AS running_first_known_sqft_filled_whole_tier,
        COUNT(*) AS cnt,
        APPROX_PERCENTILE(days_inventory_last_restock_to_opened_date_bucket, ARRAY(0.25, 0.5, 0.75)) AS days_inventory_last_restock_to_opened_date_bucket_quartiles,
        ROUND(AVG(first_known_sqft_filled), 2) AS avg_first_known_sqft_filled
    FROM ranked
    GROUP BY CASE WHEN first_known_sqft_filled_bucket >= 9 THEN 'HIGH' ELSE 'LOW' END, city, CASE WHEN running_first_known_sqft_filled_whole >= 354 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    first_known_sqft_filled_bucket_tier,
    city,
    running_first_known_sqft_filled_whole_tier
FROM daily
ORDER BY running_first_known_sqft_filled_whole_tier;
