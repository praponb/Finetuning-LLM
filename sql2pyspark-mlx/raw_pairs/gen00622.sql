WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        order_id,
        product_id,
        unit_price
    FROM order_items
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity DESC NULLS FIRST, id) = 1
),
with_dims AS (
    -- Attach products attributes
    SELECT
        a.id,
        a.product_id,
        a.unit_price,
        p.category
    FROM ranked a
    JOIN products p
        ON a.product_id = p.id
),
joined AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.product_id,
        s.unit_price,
        s.category,
        b.cost,
        b.category AS product_category,
        b.launch_date
    FROM with_dims s
    LEFT JOIN products b
        ON s.product_id <=> b.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY MONTH(launch_date) ORDER BY cost, id) AS cost_bucket,
        STDDEV_SAMP(cost) OVER (PARTITION BY product_id, product_category) AS product_id_stddev_samp_cost,
        AVG(unit_price) OVER (PARTITION BY DATE_TRUNC('YEAR', launch_date), category ORDER BY unit_price, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS unit_price_avg_5,
        DENSE_RANK() OVER (PARTITION BY product_id, product_category ORDER BY unit_price DESC NULLS FIRST) AS unit_price_dense_rank,
        LEAD(unit_price, 1, 0) OVER (ORDER BY unit_price, id) AS next_unit_price
    FROM joined
    WHERE category = 'Sports'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_id,
        unit_price,
        cost_bucket,
        product_id_stddev_samp_cost,
        unit_price_avg_5,
        unit_price_dense_rank,
        next_unit_price,
        LEAD(cost, 1) OVER (PARTITION BY category ORDER BY cost, id) AS next_cost,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price_avg_5 DESC, id) AS event_seq,
        ROUND(next_unit_price / SUM(next_unit_price) OVER (PARTITION BY product_category), 4) AS next_unit_price_share
    FROM scored
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price, id) AS latest_rank
    FROM metrics
    QUALIFY latest_rank = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    product_id,
    metric,
    metric_value
FROM (
    SELECT
        id,
        product_id,
        next_cost,
        next_unit_price_share,
        unit_price
    FROM deduped
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR metric IN (next_cost AS NEXT_COST, next_unit_price_share AS NEXT_UNIT_PRICE_SHARE_VAL, unit_price AS UNIT_PRICE)
);
