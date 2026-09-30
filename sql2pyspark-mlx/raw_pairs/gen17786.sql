WITH with_dims AS (
    -- Attach orders attributes
    SELECT e.*, b.discount
    FROM shipments e
    LEFT JOIN orders b
        ON e.order_id = b.id
),
with_dims_shipments AS (
    -- Attach orders attributes
    SELECT s.*, d.status AS order_status
    FROM with_dims s
    INNER JOIN orders d
        ON s.order_id = d.id
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_status, order_id ORDER BY cost NULLS LAST, id) AS rn
    FROM with_dims_shipments
    QUALIFY rn = 1
),
ranked_shipments AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        order_status
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_status, order_id ORDER BY discount DESC, id) AS rn_2
        FROM ranked
    ) t
    WHERE rn_2 = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        delivered_date,
        cost,
        order_status,
        NTILE(5) OVER (PARTITION BY carrier ORDER BY cost, id) cost_bucket,
        NTILE(10) OVER (PARTITION BY order_status, order_id ORDER BY cost DESC, id) cost_bucket_2,
        RANK() OVER (PARTITION BY carrier ORDER BY cost DESC) cost_rank
    FROM ranked_shipments
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        cost,
        order_status,
        cost_bucket,
        cost_bucket_2,
        cost_rank,
        ROUND(cost_bucket_2 / SUM(cost_bucket_2) OVER (PARTITION BY DATE_TRUNC('YEAR', delivered_date), order_status), 4) cost_bucket_2_share,
        NTILE(10) OVER (PARTITION BY order_id ORDER BY cost_rank DESC, id) cost_rank_bucket,
        AVG(cost) OVER (PARTITION BY order_id ORDER BY cost_bucket_2, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) cost_avg_5
    FROM scored
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY order_status ORDER BY cost_bucket_2 DESC NULLS FIRST) cost_bucket_2_dense_rank,
        PERCENT_RANK() OVER (ORDER BY cost_rank NULLS LAST) cost_rank_percent_rank
    FROM enriched
    WHERE cost_avg_5 != 94
)
SELECT
    id,
    cost,
    cost_bucket,
    cost_bucket_2,
    cost_rank,
    cost_rank_bucket,
    cost_bucket_2_dense_rank,
    cost_rank_percent_rank
FROM metrics
