WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        k,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) e AS k, attr_value
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ARRAY_CONTAINS(coupon_codes, 'WELCOME5') is_welcome5
    FROM exploded
    WHERE k IS NOT NULL
      AND k IN ('weight_kg', 'device')
),
filtered_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, SIZE(coupon_codes) coupon_codes_count
    FROM filtered
    WHERE k IS NOT NULL
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        coupon_codes,
        k,
        attr_value,
        is_welcome5,
        coupon_codes_count
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY coupon_codes_count ASC, id ASC, k ASC) AS row_num
        FROM filtered_carts
    ) t
    WHERE row_num = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        k,
        attr_value,
        is_welcome5,
        coupon_codes_count,
        SORT_ARRAY(coupon_codes) sorted_coupon_codes,
        COALESCE(k, 'N/A') k_filled,
        COALESCE(k, 'DIRECT') k_filled_2
    FROM latest
    WHERE k IN ('device', 'gift', 'weight_kg')
)
-- Grouped metrics
SELECT
    CASE WHEN coupon_codes_count >= 80 THEN 'HIGH' ELSE 'LOW' END AS coupon_codes_count_tier,
    SUM(CASE WHEN k = 'weight_kg' THEN coupon_codes_count ELSE 0 END) AS weight_kg_coupon_codes_count,
    PERCENTILE_APPROX(coupon_codes_count, ARRAY(0.25, 0.5, 0.75)) AS coupon_codes_count_quartiles
FROM cleaned
GROUP BY CASE WHEN coupon_codes_count >= 80 THEN 'HIGH' ELSE 'LOW' END
ORDER BY coupon_codes_count_tier ASC;
