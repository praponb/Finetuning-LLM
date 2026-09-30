WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        ROUND(unit_price, 1) AS unit_price_rounded
    FROM order_items
    WHERE product_id IS NOT NULL
      AND product_id IN (4, 10, 19, 34)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        quantity,
        unit_price,
        unit_price_rounded,
        ROUND(unit_price * 0.9, 2) AS unit_price_scaled
    FROM valid
    WHERE product_id IS NOT NULL
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(unit_price_rounded, 0) AS unit_price_rounded_filled
    FROM cleaned
    WHERE product_id IS NOT NULL
      AND unit_price_scaled BETWEEN 27 AND 32
),
with_dims AS (
    -- Attach products attributes
    SELECT
        a.*,
        p.price,
        p.brand
    FROM base a
    LEFT JOIN products p
        ON a.product_id <=> p.id
)
-- Window metrics over several different partitions
SELECT
    id,
    quantity,
    unit_price,
    unit_price_scaled,
    unit_price_rounded_filled,
    price,
    brand,
    CUME_DIST() OVER (PARTITION BY brand ORDER BY unit_price_scaled) AS unit_price_scaled_cume_dist,
    LAST_VALUE(unit_price_scaled) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price_scaled,
    LEAD(unit_price_rounded, 1) OVER (PARTITION BY product_id, brand ORDER BY unit_price_rounded_filled, id) AS next_unit_price_rounded,
    SUM(unit_price_scaled) OVER (PARTITION BY brand, product_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price_scaled
FROM with_dims;
