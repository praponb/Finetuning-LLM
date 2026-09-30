WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_id,
        ROUND(quantity, 2) AS quantity_rounded,
        CAST(unit_price AS INT) AS unit_price_int
    FROM order_items
    WHERE product_id = 10
),
base_order_items AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(quantity_rounded * 1.15, 2) AS quantity_rounded_adjusted
    FROM base
    WHERE unit_price_int IS NOT NULL
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM base_order_items
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity_rounded_adjusted DESC, id) = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(quantity_rounded) OVER (PARTITION BY product_id) AS product_id_sum_quantity_rounded,
        FIRST_VALUE(quantity_rounded) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY quantity_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity_rounded,
        AVG(unit_price_int) OVER (PARTITION BY product_id ORDER BY quantity_rounded_adjusted, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS unit_price_int_avg_5_prior
    FROM latest
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM with_windows
    EXCEPT DISTINCT
    SELECT id
    FROM order_items
    WHERE unit_price IS NOT NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN remaining_keys k ON s.id = k.id
),
enriched AS (
    -- Attach products attributes
    SELECT
        e.*,
        b.launch_date,
        b.brand,
        b.cost
    FROM verified e
    LEFT JOIN products b
        ON e.product_id = b.id
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM enriched
    INTERSECT
    SELECT id
    FROM order_items
    WHERE quantity = 8
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.id = k.id
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', launch_date) AS week_start,
    CASE WHEN unit_price_int >= 75 THEN 'HIGH' ELSE 'LOW' END AS unit_price_int_tier,
    DATE_TRUNC('MONTH', launch_date) AS activity_month,
    ROUND(AVG(cost), 2) AS avg_cost,
    COUNT_IF(brand = 'Acme') AS acme_events,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(CASE WHEN brand = 'Stark' THEN unit_price_int ELSE 0 END) AS stark_unit_price_int,
    PERCENTILE_APPROX(cost, ARRAY(0.25, 0.5, 0.75)) AS cost_quartiles,
    COUNT(DISTINCT id) AS distinct_ids_2
FROM reconciled
GROUP BY CUBE (DATE_TRUNC('WEEK', launch_date), CASE WHEN unit_price_int >= 75 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('MONTH', launch_date))
ORDER BY distinct_ids_2 DESC;
