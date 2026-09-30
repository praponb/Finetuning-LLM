WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        paid_at,
        AVG(amount) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS amount_avg_6_prior,
        AVG(amount) OVER w_part AS method_avg_amount
    FROM payments
    WINDOW w_part AS (PARTITION BY method)
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) AS prev_paid_at
    FROM scored
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        is_new_burst + island_id AS is_new_burst_plus_island_id,
        island_id / NULLIF(is_new_burst, 0) AS island_id_per_is_new_burst
    FROM islands
    WHERE paid_at >= DATE '2023-06-27'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        amount_avg_6_prior,
        method_avg_amount,
        prev_paid_at,
        is_new_burst,
        is_new_burst_plus_island_id,
        island_id_per_is_new_burst,
        FIRST_VALUE(amount_avg_6_prior) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY is_new_burst, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount_avg_6_prior,
        LEAD(amount_avg_6_prior, 1) OVER (PARTITION BY DATE_TRUNC('MONTH', prev_paid_at), order_id ORDER BY amount_avg_6_prior, id) AS next_amount_avg_6_prior,
        AVG(island_id) OVER (PARTITION BY order_id) AS order_id_avg_island_id,
        ROW_NUMBER() OVER (PARTITION BY MONTH(paid_at), order_id ORDER BY island_id, id) AS seq_no
    FROM cleaned
),
cleaned_payments AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, next_amount_avg_6_prior - is_new_burst_plus_island_id AS next_amount_avg_6_prior_minus_is_new_burst_plus_island_id
    FROM enriched
    WHERE is_new_burst < 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    metric_name,
    reading
FROM (
    SELECT
        id,
        is_new_burst_plus_island_id,
        island_id_per_is_new_burst
    FROM cleaned_payments
) src
UNPIVOT (
    reading FOR metric_name IN (is_new_burst_plus_island_id, island_id_per_is_new_burst)
) unp;
