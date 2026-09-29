WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, quantity - unit_price AS line_total
    FROM order_items
    WHERE quantity IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER w_ordered AS line_total_bucket,
        NTILE(4) OVER (PARTITION BY product_id ORDER BY unit_price DESC, id) AS unit_price_bucket,
        FIRST_VALUE(unit_price) IGNORE NULLS OVER (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price
    FROM valid
    WINDOW w_ordered AS (ORDER BY line_total, id)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(quantity AS DOUBLE) AS quantity_num
    FROM scored
    WHERE product_id IS NOT NULL
      AND order_id = 31
),
enriched AS (
    -- Attach products attributes
    SELECT src.*, b.brand
    FROM filtered src
    LEFT JOIN products b
        ON src.product_id = b.id
)
SELECT
    id,
    order_id,
    product_id,
    quantity,
    unit_price,
    line_total,
    unit_price_bucket,
    first_known_unit_price
FROM enriched
ORDER BY first_known_unit_price;
