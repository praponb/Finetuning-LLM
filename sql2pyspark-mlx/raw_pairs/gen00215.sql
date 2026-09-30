WITH enriched AS (
    -- Attach stores attributes
    SELECT
        e.id,
        e.store_id,
        e.quantity,
        e.last_restock,
        d.region
    FROM inventory e
    JOIN stores d
        ON e.store_id = d.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(region) IGNORE NULLS OVER (PARTITION BY YEAR(last_restock) ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_region,
        MIN(quantity) OVER (PARTITION BY region) AS region_min_quantity,
        NTILE(5) OVER (PARTITION BY region ORDER BY quantity DESC, id) AS quantity_bucket
    FROM enriched
    WHERE store_id IN (2, 3, 4, 6)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY quantity_bucket, id) AS latest_rank
    FROM metrics
    QUALIFY latest_rank = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        quantity,
        last_restock,
        region,
        first_known_region,
        LAG(region_min_quantity, 2) OVER w_grp AS prev_region_min_quantity,
        SUM(quantity) OVER (PARTITION BY YEAR(last_restock), store_id ORDER BY quantity_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity
    FROM latest
    WINDOW w_grp AS (PARTITION BY DATE_TRUNC('month', last_restock), region ORDER BY latest_rank, id)
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        quantity,
        last_restock,
        region,
        first_known_region,
        DATE_ADD(last_restock, 74) AS last_restock_plus_74d,
        COALESCE(cumulative_quantity, 0) AS cumulative_quantity_filled
    FROM with_windows
    WHERE last_restock > DATE '2023-09-06'
      AND cumulative_quantity = 660
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', last_restock) AS activity_month,
    region,
    ROUND(SUM(quantity) / NULLIF(COUNT(CASE WHEN region = 'North' THEN 1 END), 0), 2) AS quantity_per_north,
    SUM(CASE WHEN region = 'East' THEN cumulative_quantity_filled ELSE 0 END) AS east_cumulative_quantity_filled,
    STDDEV_SAMP(quantity) AS quantity_stddev,
    COUNT(*) FILTER (WHERE region = 'East') AS east_rows,
    BIT_AND(quantity) AS quantity_bit_and
FROM base
GROUP BY DATE_TRUNC('MONTH', last_restock), region
HAVING STDDEV_SAMP(quantity) < 1000000;
