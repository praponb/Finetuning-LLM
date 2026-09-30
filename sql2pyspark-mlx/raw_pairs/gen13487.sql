WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY quantity ASC) AS dedup_rank
    FROM order_items
    QUALIFY dedup_rank = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(unit_price) OVER (PARTITION BY product_id ORDER BY quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_unit_price,
        SUM(unit_price) OVER (PARTITION BY product_id ORDER BY quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_unit_price,
        MAX(unit_price) OVER (PARTITION BY product_id ORDER BY quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_unit_price,
        AVG(quantity) OVER (PARTITION BY product_id, order_id ORDER BY quantity ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS quantity_avg_5_prior
    FROM deduped
    WHERE order_id IN (3, 22, 24, 26)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(running_avg_unit_price / unit_price, 2) AS running_avg_unit_price_per_unit_price
    FROM metrics
    WHERE product_id IS NOT NULL
      AND running_sum_unit_price < 500
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CORR(quantity, unit_price) OVER (PARTITION BY order_id) AS corr_quantity_unit_price,
        LAG(running_max_unit_price, 1) OVER (ORDER BY running_avg_unit_price_per_unit_price ASC, id ASC) AS prev_running_max_unit_price,
        LEAD(quantity_avg_5_prior, 1) OVER (PARTITION BY order_id ORDER BY quantity ASC, id ASC) AS next_quantity_avg_5_prior
    FROM base
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY prev_running_max_unit_price DESC NULLS FIRST, id ASC) = 1
)
SELECT
    id,
    order_id,
    product_id,
    quantity,
    unit_price,
    dedup_rank,
    running_avg_unit_price,
    running_max_unit_price,
    running_avg_unit_price_per_unit_price
FROM ranked
ORDER BY running_avg_unit_price_per_unit_price ASC NULLS LAST
