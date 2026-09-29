WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_id,
        sale_date,
        units,
        DATE_ADD(sale_date, 64) AS sale_date_plus_64d
    FROM store_sales
    WHERE sale_date >= TO_DATE('2023-04-24')
      AND units >= 17
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(units) OVER (PARTITION BY product_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_units,
        MIN(units) OVER (PARTITION BY product_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_units,
        SUM(units) OVER (PARTITION BY product_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_units,
        COUNT(units) OVER (PARTITION BY product_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_units,
        NTILE(10) OVER (ORDER BY units DESC, id) AS units_bucket
    FROM cleaned
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY running_sum_units DESC, id) AS row_num
        FROM metrics
    ) ranked
    WHERE row_num = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER (ORDER BY running_max_units DESC, id) AS running_max_units_bucket,
        ROUND(running_sum_units / SUM(running_sum_units) OVER (PARTITION BY DATE_TRUNC('YEAR', sale_date), product_id), 4) AS running_sum_units_share
    FROM deduped
),
enriched_store_sales AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(running_sum_units) OVER (PARTITION BY product_id ORDER BY running_sum_units_share, id) AS next_running_sum_units,
        LEAD(units_bucket) OVER (PARTITION BY product_id ORDER BY running_sum_units_share, id) AS next_units_bucket,
        LAG(units_bucket) OVER (PARTITION BY product_id ORDER BY running_sum_units_share, id) AS prev_units_bucket,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY running_sum_units_share, id) AS event_seq
    FROM enriched
)
SELECT
    id,
    product_id,
    sale_date,
    units,
    sale_date_plus_64d,
    running_min_units,
    running_count_units,
    units_bucket,
    row_num
FROM enriched_store_sales
ORDER BY running_count_units DESC;
