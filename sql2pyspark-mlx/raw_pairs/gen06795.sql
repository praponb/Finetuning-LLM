WITH filtered AS (
    SELECT *, ROUND(unit_price, 0) AS unit_price_rounded
    FROM order_items
    WHERE product_id IS NOT NULL
      AND order_id IN (1, 15, 16)
),
valid AS (
    SELECT
        id,
        product_id,
        quantity,
        unit_price_rounded,
        CAST(unit_price_rounded AS INT) AS unit_price_rounded_int
    FROM filtered
    WHERE unit_price_rounded BETWEEN 67 AND 73
),
latest AS (
    SELECT
        id,
        product_id,
        quantity
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity NULLS FIRST, id) = 1
),
cleaned AS (
    SELECT *, ROUND(quantity * 1.1, 2) AS quantity_adjusted
    FROM latest
    WHERE product_id IS NOT NULL
),
daily AS (
    SELECT
        product_id,
        MAX(quantity_adjusted) AS max_quantity_adjusted,
        COUNT(*) AS row_count,
        PERCENTILE_APPROX(quantity_adjusted, 0.5) AS quantity_adjusted_p50,
        PERCENTILE_APPROX(quantity, 0.9) AS quantity_p90,
        COUNT(*) AS event_count,
        BIT_AND(quantity) AS quantity_bit_and
    FROM cleaned
    GROUP BY 1
)
SELECT
    product_id,
    max_quantity_adjusted,
    quantity_adjusted_p50,
    quantity_p90,
    event_count,
    quantity_bit_and
FROM daily
ORDER BY quantity_p90 NULLS FIRST
