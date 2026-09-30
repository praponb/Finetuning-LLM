WITH base AS (
    SELECT *, WEEKOFYEAR(sale_date) AS sale_date_weekofyear
    FROM store_sales
    WHERE sale_date >= DATE '2023-06-28'
      AND store_id IN (2, 4, 5, 6)
),
with_dims AS (
    SELECT
        e.id,
        e.product_id,
        e.units,
        e.revenue,
        e.sale_date_weekofyear,
        d.launch_date,
        d.brand,
        d.cost
    FROM base e
    LEFT JOIN products d
        ON e.product_id = d.id
),
with_windows AS (
    SELECT
        id,
        revenue,
        sale_date_weekofyear,
        launch_date,
        brand,
        cost,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY units, id) AS row_seq,
        ROUND(cost / SUM(cost) OVER (PARTITION BY brand), 4) AS cost_share
    FROM with_dims
),
enriched AS (
    SELECT
        id,
        brand,
        LAG(cost, 1) OVER (PARTITION BY YEAR(launch_date), brand ORDER BY cost, id) AS prev_cost,
        NTILE(3) OVER w_grp AS row_seq_bucket,
        AVG(revenue) OVER (PARTITION BY brand ORDER BY cost, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS revenue_avg_3_prior
    FROM with_windows
    WHERE brand = 'Stark'
    WINDOW w_grp AS (PARTITION BY MONTH(launch_date) ORDER BY row_seq, id)
)
SELECT
    id,
    brand,
    revenue_avg_3_prior
FROM enriched
ORDER BY id NULLS LAST;
