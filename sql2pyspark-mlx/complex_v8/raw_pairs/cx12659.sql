WITH recent AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        COALESCE(premium, 0.0) AS premium_filled,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref
    FROM policies
    WHERE start_date IS NOT NULL
      AND status IN ('EXPIRED', 'CANCELLED', 'ACTIVE')
),
stats AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', end_date) AS end_date_month,
        product,
        MAX(premium) AS max_premium,
        MAX(premium_filled) AS max_premium_filled,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT product) AS distinct_products
    FROM recent
    GROUP BY DATE_TRUNC('MONTH', end_date), product
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER w_distinct_ids_by_distinct_pro AS distinct_products_dense_rank,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY distinct_ids, YEAR(end_date_month))) AS distinct_product_in_distinct_ids_end_date_month_,
        ROW_NUMBER() OVER (PARTITION BY distinct_ids ORDER BY distinct_products ASC, end_date_month, product) AS seq_no,
        ROUND(max_premium_filled / NULLIF(SUM(max_premium_filled) OVER (PARTITION BY distinct_ids, YEAR(end_date_month)), 0), 4) AS max_premium_filled_share_of_distinct_ids_end_dat,
        NTILE(4) OVER (PARTITION BY distinct_ids ORDER BY distinct_products ASC, end_date_month, product) AS distinct_products_quartile
    FROM stats
    WINDOW w_distinct_ids_by_distinct_pro AS (PARTITION BY distinct_ids ORDER BY distinct_products ASC)
)
-- Native SQL UNPIVOT (wide to long)
SELECT end_date_month, product, metric_name, metric_value
FROM (
    SELECT end_date_month, product, distinct_product_in_distinct_ids_end_date_month_, seq_no
    FROM ranked
) src
UNPIVOT (
    metric_value FOR metric_name IN (distinct_product_in_distinct_ids_end_date_month_, seq_no)
) unp
ORDER BY end_date_month, product, metric_name
