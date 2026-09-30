WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        CORR(revenue, units) OVER (PARTITION BY store_id) AS corr_revenue_units,
        NTILE(4) OVER (ORDER BY revenue ASC, id ASC) AS revenue_bucket
    FROM store_sales
)
SELECT id, store_id
FROM with_windows
ORDER BY id ASC
