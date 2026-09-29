WITH with_windows AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        SUM(revenue) OVER w_recent AS running_revenue,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY store_id), 4) AS revenue_share,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue ASC, id ASC) AS seq_no,
        NTILE(3) OVER (PARTITION BY product_id ORDER BY revenue DESC, id ASC) AS revenue_bucket,
        RANK() OVER (PARTITION BY store_id ORDER BY revenue ASC) AS revenue_rank
    FROM store_sales
    WINDOW w_recent AS (PARTITION BY product_id ORDER BY units ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    product_id,
    sale_date,
    units,
    revenue,
    running_revenue,
    revenue_share
FROM with_windows
ORDER BY revenue_share DESC NULLS FIRST
