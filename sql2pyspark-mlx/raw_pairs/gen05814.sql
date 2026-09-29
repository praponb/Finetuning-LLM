WITH valid AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        DAYOFWEEK(sale_date) AS sale_date_dayofweek,
        ROUND(revenue / units, 2) AS revenue_per_unit
    FROM store_sales
    WHERE product_id IN (1, 6, 8, 10)
),
enriched AS (
    SELECT
        *,
        AVG(units) OVER w_grp AS last7_avg_units,
        MAX(units) OVER w_grp AS last7_max_units,
        COUNT(units) OVER w_grp AS last7_count_units
    FROM valid
    WINDOW w_grp AS (PARTITION BY store_id, product_id ORDER BY units, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
enriched_store_sales AS (
    SELECT
        id,
        store_id,
        product_id,
        revenue,
        revenue_per_unit,
        AVG(sale_date_dayofweek) OVER w_ordered AS sale_date_dayofweek_avg_6_prior,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue_per_unit DESC, id) AS seq_no
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY product_id ORDER BY sale_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue_per_unit DESC, id) AS latest_rank
    FROM enriched_store_sales
    QUALIFY latest_rank = 1
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date_dayofweek_avg_6_prior NULLS FIRST, id) AS latest_rank_2
    FROM ranked
    QUALIFY latest_rank_2 = 1
)
SELECT
    id,
    product_id,
    revenue,
    revenue_per_unit,
    seq_no,
    latest_rank,
    latest_rank_2
FROM latest
ORDER BY revenue_per_unit DESC
