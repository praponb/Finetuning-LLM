WITH with_windows AS (
    SELECT
        id,
        store_id,
        units,
        revenue,
        NTILE(5) OVER (PARTITION BY store_id ORDER BY revenue DESC, id) AS revenue_bucket,
        CUME_DIST() OVER (PARTITION BY MONTH(sale_date), product_id ORDER BY revenue DESC NULLS LAST) AS revenue_cume_dist,
        LAG(revenue, 1) OVER (PARTITION BY product_id, store_id ORDER BY revenue, id) AS prev_revenue
    FROM store_sales
),
with_windows_store_sales AS (
    SELECT
        *,
        AVG(revenue_bucket) OVER (ORDER BY units, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS revenue_bucket_avg_2,
        NTILE(4) OVER (PARTITION BY store_id ORDER BY revenue_bucket, id) AS revenue_bucket_bucket
    FROM with_windows
),
matched_keys AS (
    SELECT id, store_id
    FROM with_windows_store_sales
    EXCEPT ALL
    SELECT id, store_id
    FROM store_sales
    WHERE MONTH(sale_date) IN (4, 7, 10)
),
reconciled AS (
    SELECT s.*
    FROM with_windows_store_sales s
    INNER JOIN matched_keys k ON s.id = k.id AND s.store_id = k.store_id
),
joined AS (
    SELECT
        a.store_id,
        a.revenue,
        a.revenue_cume_dist,
        a.prev_revenue,
        a.revenue_bucket_bucket,
        p.opened_date
    FROM reconciled a
    LEFT JOIN stores p
        ON a.store_id = p.id
),
joined_store_sales AS (
    SELECT
        src.*,
        d.opened_date AS store_opened_date,
        d.store_name
    FROM joined src
    LEFT JOIN stores d
        ON src.store_id = d.id
),
with_windows_store_sales_2 AS (
    SELECT
        *,
        COUNT(revenue_cume_dist) OVER (PARTITION BY store_id) AS store_id_count_revenue_cume_dist,
        CUME_DIST() OVER (ORDER BY revenue NULLS LAST) AS revenue_cume_dist_2
    FROM joined_store_sales
    WHERE store_name LIKE '%Rive%'
),
joined_store_sales_2 AS (
    SELECT
        e.store_id,
        e.prev_revenue,
        b.opened_date AS store_opened_date_2,
        b.city,
        b.sqft
    FROM with_windows_store_sales_2 e
    INNER JOIN stores b
        ON e.store_id = b.id
)
SELECT
    DATE_TRUNC('MONTH', store_opened_date_2) AS activity_month,
    APPROX_PERCENTILE(sqft, 0.5) AS sqft_p50,
    COUNT(DISTINCT store_id) AS distinct_store_ids,
    GROUPING_ID() AS gid
FROM joined_store_sales_2
GROUP BY CUBE (DATE_TRUNC('MONTH', store_opened_date_2));
