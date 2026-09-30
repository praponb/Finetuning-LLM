WITH flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        customer_id,
        coupon_codes,
        attributes,
        COALESCE(attributes['device'], 'unknown') AS device_or_default,
        SIZE(FILTER(items, i -> i.qty >= 3)) AS bulk_lines
    FROM carts
    WHERE customer_id = 3
)
-- Ranking and gap features
SELECT
    *,
    LAST_VALUE(bulk_lines) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY bulk_lines, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_bulk_lines,
    NTILE(3) OVER (PARTITION BY customer_id ORDER BY bulk_lines DESC, id) AS bulk_lines_bucket,
    MAX(bulk_lines) OVER (PARTITION BY customer_id) AS customer_id_max_bulk_lines,
    LAG(bulk_lines, 2) OVER (ORDER BY bulk_lines, id) AS prev_bulk_lines
FROM flattened_attrs
