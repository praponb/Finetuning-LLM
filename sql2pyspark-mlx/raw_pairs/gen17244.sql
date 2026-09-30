WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, unit_price * quantity AS line_total
    FROM order_items
    WHERE product_id IN (15, 20, 35)
)
-- Ranking and gap features
SELECT
    id,
    product_id,
    unit_price,
    line_total,
    FIRST_VALUE(line_total) IGNORE NULLS OVER (PARTITION BY order_id, product_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_line_total,
    NTILE(10) OVER (PARTITION BY product_id ORDER BY line_total, id) AS line_total_bucket,
    RANK() OVER (ORDER BY quantity NULLS LAST) AS quantity_rank
FROM cleaned;
