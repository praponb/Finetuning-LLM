WITH enriched AS (
    SELECT
        s.id,
        s.store_id,
        s.product_id,
        s.sale_date,
        s.revenue,
        p.city,
        p.sqft,
        p.opened_date
    FROM store_sales s
    LEFT JOIN stores p
        ON s.store_id = p.id
),
metrics AS (
    SELECT
        id,
        store_id,
        sale_date,
        sqft,
        opened_date,
        AVG(revenue) OVER (PARTITION BY product_id ORDER BY sale_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) revenue_avg_6_prior,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY store_id), 4) sqft_share
    FROM enriched
),
base AS (
    SELECT *, COALESCE(sqft_share, 0) sqft_share_filled
    FROM metrics
    WHERE sale_date >= TO_DATE('2023-04-09')
),
scored AS (
    SELECT
        *,
        AVG(sqft_share) OVER (PARTITION BY store_id ORDER BY revenue_avg_6_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_sqft_share,
        MAX(sqft_share) OVER (PARTITION BY store_id ORDER BY revenue_avg_6_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_sqft_share,
        SUM(sqft_share) OVER (PARTITION BY store_id ORDER BY revenue_avg_6_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum_sqft_share
    FROM base
)
SELECT *
FROM (
    SELECT
        id,
        store_id,
        running_sum_sqft_share,
        running_avg_sqft_share
    FROM scored
) src
UNPIVOT (
    value FOR measure IN (running_sum_sqft_share, running_avg_sqft_share)
)
