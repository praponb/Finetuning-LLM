WITH valid AS (
    SELECT
        id,
        store_name,
        city,
        region,
        sqft,
        COALESCE(sqft, 0) AS sqft_filled
    FROM stores
    WHERE sqft >= 7611
),
latest_match AS (
    SELECT
        s.id,
        s.store_name,
        s.region,
        s.sqft_filled,
        top1.order_order_date,
        top1.discount
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.order_date AS order_order_date, c.discount
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
valid_stores AS (
    SELECT
        id,
        region,
        sqft_filled,
        discount,
        CONCAT_WS(',', store_name, region) AS store_name_region_key,
        sqft_filled - discount AS sqft_filled_minus_discount,
        COALESCE(region, 'N/A') AS region_clean
    FROM latest_match
    WHERE region IS NOT NULL
),
matched_keys AS (
    SELECT id
    FROM valid_stores
    INTERSECT
    SELECT id
    FROM stores
    WHERE sqft IS NULL
),
verified AS (
    SELECT s.*
    FROM valid_stores s
    INNER JOIN matched_keys k ON s.id = k.id
),
cleaned AS (
    SELECT
        *,
        UPPER(region) AS region_upper,
        COALESCE(region, 'UNKNOWN') AS resolved_region
    FROM verified
    WHERE region IS NOT NULL
      AND sqft_filled_minus_discount <= 53
),
scored AS (
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY region ORDER BY sqft_filled_minus_discount DESC) AS sqft_filled_minus_discount_percent_rank,
        MIN(discount) OVER (PARTITION BY region) AS region_min_discount,
        SUM(sqft_filled) OVER (PARTITION BY region) AS region_sum_sqft_filled
    FROM cleaned
),
not_flagged AS (
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.product_id IN (4, 7)
        )
)
SELECT
    id,
    sqft_filled,
    store_name_region_key,
    sqft_filled_minus_discount,
    region_upper,
    resolved_region,
    sqft_filled_minus_discount_percent_rank,
    region_min_discount
FROM not_flagged
ORDER BY sqft_filled ASC NULLS LAST;
