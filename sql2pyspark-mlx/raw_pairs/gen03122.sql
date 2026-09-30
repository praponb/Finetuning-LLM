WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(cost / SUM(cost) OVER (PARTITION BY carrier), 4) AS cost_share,
        CUME_DIST() OVER (PARTITION BY order_id ORDER BY cost NULLS LAST) AS cost_cume_dist,
        LAST_VALUE(carrier) IGNORE NULLS OVER (PARTITION BY carrier ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_carrier,
        FIRST_VALUE(carrier) IGNORE NULLS OVER (PARTITION BY status ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_carrier
    FROM shipments
    WHERE carrier IN ('Flash', 'UPS')
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status, carrier ORDER BY delivered_date DESC NULLS FIRST, id) AS dedup_rank
        FROM with_windows
    ) ranked
    WHERE dedup_rank = 1
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, dedup_rank * cost AS dedup_rank_times_cost
    FROM deduped
    WHERE status = 'LOST'
),
enriched AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.cost,
        e.status,
        e.cost_share,
        e.cost_cume_dist,
        e.last_known_carrier,
        e.first_known_carrier,
        e.dedup_rank,
        e.dedup_rank_times_cost,
        o.amount,
        o.shipping_fee
    FROM base e
    LEFT JOIN orders o
        ON e.order_id = o.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        cost,
        cost_cume_dist,
        last_known_carrier,
        dedup_rank,
        amount,
        shipping_fee,
        LAG(shipping_fee) OVER (PARTITION BY status, order_id ORDER BY dedup_rank_times_cost, id) AS prev_shipping_fee,
        LEAD(shipping_fee) OVER (PARTITION BY status, order_id ORDER BY dedup_rank_times_cost, id) AS next_shipping_fee,
        LEAD(cost_cume_dist) OVER (PARTITION BY status, order_id ORDER BY dedup_rank_times_cost, id) AS next_cost_cume_dist,
        LAG(cost_cume_dist) OVER (PARTITION BY status, order_id ORDER BY dedup_rank_times_cost, id) AS prev_cost_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY status, order_id ORDER BY dedup_rank_times_cost, id) AS event_seq
    FROM enriched
)
-- Running and moving aggregates
SELECT
    id,
    order_id,
    cost,
    cost_cume_dist,
    dedup_rank,
    amount,
    shipping_fee,
    next_shipping_fee,
    prev_cost_cume_dist,
    event_seq,
    LAG(prev_shipping_fee) OVER w_part AS prev_prev_shipping_fee,
    LEAD(prev_shipping_fee) OVER w_part AS next_prev_shipping_fee,
    LEAD(cost) OVER w_part AS next_cost,
    LAG(cost) OVER w_part AS prev_cost,
    ROW_NUMBER() OVER w_part AS row_seq,
    LAST_VALUE(event_seq, TRUE) OVER (PARTITION BY order_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_seq
FROM metrics
WINDOW w_part AS (PARTITION BY order_id ORDER BY cost_cume_dist, id);
