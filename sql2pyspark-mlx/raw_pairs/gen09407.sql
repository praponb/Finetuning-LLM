WITH filtered AS (
    SELECT
        id,
        product_id,
        sale_date,
        units,
        CAST(units AS DOUBLE) AS units_num
    FROM store_sales
    WHERE sale_date > TO_DATE('2023-03-02')
),
joined AS (
    SELECT
        a.*,
        p.brand,
        p.product_name,
        p.cost
    FROM filtered a
    JOIN products p
        ON a.product_id = p.id
),
enriched AS (
    SELECT
        id,
        units_num,
        brand,
        cost,
        SUM(units) OVER (PARTITION BY brand, product_id ORDER BY units_num, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_units,
        AVG(units) OVER (PARTITION BY brand, product_id ORDER BY units_num, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_units,
        MAX(units) OVER (PARTITION BY brand, product_id ORDER BY units_num, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_units
    FROM joined
),
metrics AS (
    SELECT
        *,
        AVG(last3_max_units) OVER (PARTITION BY brand ORDER BY cost, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_last3_max_units,
        MIN(last3_max_units) OVER (PARTITION BY brand ORDER BY cost, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_last3_max_units,
        MAX(last3_max_units) OVER (PARTITION BY brand ORDER BY cost, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_last3_max_units,
        NTILE(10) OVER (ORDER BY cost DESC, id) AS cost_bucket
    FROM enriched
),
kept_keys AS (
    SELECT id
    FROM metrics
    EXCEPT DISTINCT
    SELECT id
    FROM store_sales
    WHERE units >= 23
),
kept_rows AS (
    SELECT s.*
    FROM metrics s
    JOIN kept_keys k ON s.id = k.id
),
enriched_store_sales AS (
    SELECT
        *,
        ROUND(last3_max_units / SUM(last3_max_units) OVER (PARTITION BY brand), 4) AS last3_max_units_share,
        ROUND(last3_sum_units / SUM(last3_sum_units) OVER (PARTITION BY brand), 4) AS last3_sum_units_share,
        MAX(last3_min_last3_max_units) OVER w_grp AS brand_max_last3_min_last3_max_units,
        CUME_DIST() OVER (PARTITION BY brand ORDER BY cost DESC NULLS FIRST) AS cost_cume_dist,
        RANK() OVER (ORDER BY last3_max_units NULLS FIRST) AS last3_max_units_rank
    FROM kept_rows
    WINDOW w_grp AS (PARTITION BY brand)
),
base AS (
    SELECT
        *,
        CASE WHEN last3_sum_units >= 220 THEN 'High' WHEN last3_sum_units >= 28 THEN 'Medium' ELSE 'Low' END AS last3_sum_units_level,
        COALESCE(brand, 'UNKNOWN') AS resolved_brand
    FROM enriched_store_sales
    WHERE brand IS NOT NULL
      AND cost_cume_dist BETWEEN 0.23 AND 0.65
)
SELECT *
FROM (
    SELECT
        brand,
        last3_max_last3_max_units,
        units_num,
        last3_avg_units
    FROM base
) src
UNPIVOT (
    reading FOR measure IN (last3_max_last3_max_units, units_num, last3_avg_units)
);
