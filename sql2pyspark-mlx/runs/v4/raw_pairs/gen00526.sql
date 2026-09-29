WITH enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        CORR(revenue, units) OVER (PARTITION BY product_id, store_id) corr_revenue_units,
        AVG(revenue) OVER (PARTITION BY product_id ORDER BY units, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) revenue_avg_2_prior,
        SUM(units) OVER (PARTITION BY product_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_units,
        CORR(revenue, units) OVER (PARTITION BY store_id) corr_revenue_units_2,
        NTILE(5) OVER (PARTITION BY product_id ORDER BY units, id) units_bucket
    FROM store_sales
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY revenue_avg_2_prior, id) row_seq,
        FIRST_VALUE(corr_revenue_units, TRUE) OVER (PARTITION BY store_id, product_id ORDER BY corr_revenue_units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_corr_revenue_units
    FROM enriched
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY row_seq, id) AS row_num
        FROM scored
    ) ranked
    WHERE row_num = 1
),
scored_store_sales AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        corr_revenue_units,
        revenue_avg_2_prior,
        cumulative_units,
        corr_revenue_units_2,
        units_bucket,
        row_seq,
        first_known_corr_revenue_units,
        row_num,
        NTILE(10) OVER (ORDER BY units, id) units_bucket_2,
        SUM(corr_revenue_units) OVER (PARTITION BY product_id ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_corr_revenue_units,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY units NULLS LAST) units_cume_dist
    FROM ranked
)
SELECT
    s.id,
    s.store_id,
    s.sale_date,
    s.units,
    s.revenue,
    s.corr_revenue_units,
    s.cumulative_units,
    s.units_bucket,
    s.row_seq,
    (SELECT SUM(x.quantity) FROM inventory x WHERE x.store_id = s.store_id) AS sum_inventory_quantity
FROM scored_store_sales s
