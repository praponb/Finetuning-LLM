WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT id, INLINE(items)
    FROM carts
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        INITCAP(TRIM(promo)) AS promo_title,
        COALESCE(promo, 'UNKNOWN') AS promo_clean
    FROM flattened
    WHERE price BETWEEN 130 AND 220
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM base
    EXCEPT
    SELECT id
    FROM carts
    WHERE customer_id IS NOT NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN matched_keys k ON s.id = k.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        product_id,
        qty,
        price,
        promo,
        promo_title,
        promo_clean,
        GREATEST(product_id, price) AS greatest_product_id_price
    FROM kept_rows
    WHERE product_id > 4
)
SELECT
    product_id,
    price,
    promo,
    promo_clean,
    greatest_product_id_price,
    CASE WHEN promo RLIKE '^(VIP|BUNDLE)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS promo_zone
FROM valid
ORDER BY price;
