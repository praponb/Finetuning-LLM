WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, shipping_fee / NULLIF(discount, 0) AS shipping_fee_per_discount
    FROM orders
    WHERE order_date >= TO_DATE('2024-02-15')
      AND customer_id IS NOT NULL
),
remaining_keys AS (
    -- Set difference on keys
    SELECT customer_id, channel
    FROM base
    EXCEPT
    SELECT customer_id, channel
    FROM orders
    WHERE amount > 1060
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN remaining_keys k ON s.customer_id = k.customer_id AND s.channel = k.channel
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        STDDEV_SAMP(amount) OVER (PARTITION BY status) AS status_stddev_samp_amount,
        PERCENT_RANK() OVER w_part AS shipping_fee_per_discount_percent_rank,
        COUNT(discount) OVER (PARTITION BY status, store_id) AS status_count_discount
    FROM verified
    WINDOW w_part AS (ORDER BY shipping_fee_per_discount DESC)
)
-- Grouped metrics
SELECT
    DATE_TRUNC('WEEK', order_date) AS week_start,
    CASE WHEN status_stddev_samp_amount >= 1226 THEN 'HIGH' ELSE 'LOW' END AS status_stddev_samp_amount_tier,
    COUNT(DISTINCT store_id) AS distinct_store_ids,
    COUNT(*) AS row_count,
    PERCENTILE_APPROX(shipping_fee_per_discount_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS shipping_fee_per_discount_percent_rank_quartiles,
    GROUPING(DATE_TRUNC('WEEK', order_date)) AS grouping_id
FROM metrics
GROUP BY GROUPING SETS ((DATE_TRUNC('WEEK', order_date), CASE WHEN status_stddev_samp_amount >= 1226 THEN 'HIGH' ELSE 'LOW' END), (DATE_TRUNC('WEEK', order_date)), ())
HAVING distinct_store_ids > 1;
