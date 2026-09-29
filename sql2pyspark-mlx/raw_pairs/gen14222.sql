WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(method, 1, 3) AS method_prefix,
        COALESCE(method, 'OTHER') AS method_filled
    FROM payments
    WHERE order_id IS NOT NULL
      AND method IS DISTINCT FROM 'cash'
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) AS prev_paid_at
    FROM base
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        order_id,
        method,
        amount,
        status,
        method_filled,
        island_id
    FROM sessions_payments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY island_id DESC, id) = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(amount) OVER (PARTITION BY order_id ORDER BY island_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_amount,
        AVG(amount) OVER (PARTITION BY order_id ORDER BY island_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_amount,
        SUM(amount) OVER (PARTITION BY order_id ORDER BY island_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_amount,
        MAX(amount) OVER (PARTITION BY order_id ORDER BY island_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_amount,
        FIRST_VALUE(amount, TRUE) OVER (PARTITION BY status ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount
    FROM latest
),
matched_keys AS (
    -- Set difference on keys
    SELECT method, id
    FROM enriched
    EXCEPT
    SELECT method, id
    FROM payments
    WHERE status IN ('SUCCESS', 'REFUNDED', 'FAILED')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN matched_keys k ON s.method = k.method AND s.id = k.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        method,
        amount,
        status,
        method_filled,
        island_id,
        running_avg_amount,
        running_sum_amount,
        first_known_amount,
        COALESCE(running_max_amount, 0) AS running_max_amount_filled,
        running_sum_amount - island_id AS running_sum_amount_minus_island_id
    FROM kept_rows
    WHERE running_min_amount BETWEEN 940 AND 1600
),
enriched_payments AS (
    -- Window metrics over several different partitions
    SELECT
        order_id,
        method,
        amount,
        status,
        method_filled,
        island_id,
        running_avg_amount,
        running_sum_amount,
        first_known_amount,
        running_max_amount_filled,
        CUME_DIST() OVER (PARTITION BY method ORDER BY first_known_amount DESC NULLS LAST) AS first_known_amount_cume_dist,
        DENSE_RANK() OVER (PARTITION BY status, method ORDER BY running_avg_amount DESC NULLS LAST) AS running_avg_amount_dense_rank,
        ROUND(running_sum_amount / SUM(running_sum_amount) OVER (PARTITION BY order_id), 4) AS running_sum_amount_share,
        DENSE_RANK() OVER (ORDER BY island_id DESC) AS island_id_dense_rank
    FROM filtered
)
SELECT
    s.order_id,
    s.method,
    s.amount,
    s.status,
    s.island_id,
    s.running_avg_amount,
    s.running_sum_amount,
    s.first_known_amount,
    s.running_max_amount_filled,
    (SELECT SUM(x.cost) FROM shipments x WHERE x.order_id = s.order_id) AS sum_shipment_cost
FROM enriched_payments s;
