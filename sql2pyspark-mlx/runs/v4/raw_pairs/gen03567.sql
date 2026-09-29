WITH kept_keys AS (
    -- Set difference on keys
    SELECT id, product_id
    FROM store_sales
    EXCEPT
    SELECT id, product_id
    FROM store_sales
    WHERE product_id = 4
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM store_sales s
    INNER JOIN kept_keys k ON s.id = k.id AND s.product_id = k.product_id
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        store_id,
        product_id,
        sale_date,
        units,
        DAY(sale_date) AS sale_date_day,
        DATE_TRUNC('WEEK', sale_date) AS sale_date_period
    FROM reconciled
    WHERE sale_date >= DATE '2023-12-20'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        STDDEV_SAMP(sale_date_day) OVER (PARTITION BY store_id, product_id) AS store_id_stddev_samp_sale_date_day,
        DENSE_RANK() OVER (PARTITION BY store_id ORDER BY units DESC NULLS LAST) AS units_dense_rank,
        COUNT(units) OVER (PARTITION BY store_id) AS store_id_count_units
    FROM filtered
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        units_dense_rank * sale_date_day AS units_dense_rank_times_sale_date_day,
        ROUND(store_id_stddev_samp_sale_date_day / store_id_count_units, 2) AS store_id_stddev_samp_sale_date_day_per_store_id_count_units
    FROM enriched
    WHERE store_id = 2
)
-- Aggregate with conditional metrics
SELECT
    store_id,
    COUNT(*) AS num_rows,
    BIT_OR(units) AS units_bit_or
FROM valid
GROUP BY ROLLUP (store_id)
HAVING BIT_OR(units) < 1
ORDER BY units_bit_or NULLS LAST;
