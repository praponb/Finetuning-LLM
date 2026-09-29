WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_id,
        ROUND(quantity * 0.85, 2) AS adjusted_quantity,
        DATE_TRUNC('week', last_restock) AS last_restock_period
    FROM inventory
    WHERE product_id IN (3, 6, 9)
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        product_id,
        adjusted_quantity
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock_period ASC NULLS LAST, id ASC) = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(adjusted_quantity) OVER w_grp AS next_adjusted_quantity,
        LAG(adjusted_quantity) OVER w_grp AS prev_adjusted_quantity,
        ROW_NUMBER() OVER w_grp AS seq_no,
        AVG(adjusted_quantity) OVER (PARTITION BY product_id ORDER BY adjusted_quantity ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS adjusted_quantity_avg_6_prior
    FROM ranked
    WINDOW w_grp AS (PARTITION BY product_id ORDER BY adjusted_quantity ASC, id ASC)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        next_adjusted_quantity,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY adjusted_quantity DESC, id ASC) AS seq_no_2,
        AVG(prev_adjusted_quantity) OVER (PARTITION BY product_id ORDER BY next_adjusted_quantity ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS prev_adjusted_quantity_avg_2_prior
    FROM with_windows
    WHERE adjusted_quantity BETWEEN 22 AND 65
),
joined AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.product_id,
        s.next_adjusted_quantity,
        s.seq_no_2,
        s.prev_adjusted_quantity_avg_2_prior,
        b.product_name
    FROM enriched s
    INNER JOIN products b
        ON s.product_id = b.id
),
summary AS (
    -- Multi-level aggregation
    SELECT
        product_id,
        MAX(prev_adjusted_quantity_avg_2_prior) AS max_prev_adjusted_quantity_avg_2_prior,
        COUNT(*) AS cnt,
        COUNT(DISTINCT id) AS distinct_ids,
        PERCENTILE_APPROX(next_adjusted_quantity, 0.9) AS next_adjusted_quantity_p90
    FROM joined
    GROUP BY product_id
)
SELECT
    s.product_id,
    s.distinct_ids,
    s.next_adjusted_quantity_p90,
    (SELECT MAX(t2.unit_price) FROM order_items t2 WHERE t2.product_id = s.product_id) AS max_order_item_unit_price
FROM summary s
ORDER BY next_adjusted_quantity_p90 DESC, distinct_ids ASC;
