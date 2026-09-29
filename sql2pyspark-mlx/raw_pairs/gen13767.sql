WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        LAST_VALUE(units) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_units,
        NTILE(3) OVER (PARTITION BY product_id ORDER BY units DESC, id) AS units_bucket,
        LEAD(units, 1) OVER w_recent AS next_units,
        LAST_VALUE(revenue, TRUE) OVER (PARTITION BY YEAR(sale_date), product_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_revenue,
        LAG(units, 1) OVER w_recent AS prev_units
    FROM store_sales
    WINDOW w_recent AS (PARTITION BY product_id ORDER BY revenue, id)
)
SELECT
    id,
    sale_date,
    units,
    revenue,
    last_known_units,
    units_bucket,
    next_units,
    last_known_revenue,
    prev_units
FROM enriched
ORDER BY units DESC NULLS FIRST;
