WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        line.product_id,
        line.promo
    FROM (
        SELECT
            id,
            customer_id,
            coupon_codes,
            EXPLODE_OUTER(items) AS line
        FROM carts
    ) x
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        coupon_codes,
        product_id,
        SUBSTRING(promo, 1, 4) AS promo_prefix,
        COALESCE(promo, 'Unknown') AS promo_filled,
        COALESCE(promo, 'NONE') AS promo_filled_2
    FROM exploded
    WHERE promo IS NOT NULL
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.*,
        b.city,
        b.age,
        b.country
    FROM base e
    LEFT JOIN customers b
        ON e.customer_id = b.id
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ARRAY_CONTAINS(coupon_codes, 'BUNDLE') AS is_bundle,
        COALESCE(city, 'NONE') AS city_clean
    FROM with_dims
    WHERE ARRAY_CONTAINS(coupon_codes, 'WELCOME5')
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        coupon_codes,
        promo_prefix,
        age,
        country,
        is_bundle,
        city_clean,
        CASE WHEN is_bundle THEN 'Yes' ELSE 'No' END AS is_bundle_label,
        COALESCE(city, 'UNKNOWN') AS city_filled
    FROM valid
    WHERE product_id >= 6
),
base_carts AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SORT_ARRAY(coupon_codes) AS sorted_coupon_codes,
        COALESCE(country, 'N/A') AS country_filled
    FROM cleaned
    WHERE ARRAY_CONTAINS(coupon_codes, 'VIP')
),
by_type AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            customer_id,
            country,
            age
        FROM base_carts
    ) src
    PIVOT (
        SUM(age)
        FOR country IN ('UK', 'US', 'SG')
    )
)
SELECT
    customer_id,
    UK,
    US
FROM by_type
ORDER BY UK, US;
