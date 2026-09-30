WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY product_id), 4) AS revenue_share,
        SUM(units) OVER (PARTITION BY product_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_units,
        FIRST_VALUE(units, TRUE) OVER (PARTITION BY product_id, store_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_units,
        LEAD(units, 2) OVER (ORDER BY revenue, id) AS next_units
    FROM store_sales
)
SELECT
    s.id,
    s.product_id,
    s.units,
    s.revenue,
    s.running_units,
    s.first_known_units,
    s.next_units,
    (SELECT MIN(x.units) FROM store_sales x WHERE x.product_id = s.product_id) AS min_store_sale_units
FROM enriched s
ORDER BY revenue
