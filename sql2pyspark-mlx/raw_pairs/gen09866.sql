WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        method,
        status,
        STDDEV_SAMP(amount) OVER (PARTITION BY order_id) AS order_id_stddev_samp_amount,
        AVG(amount) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS amount_avg_2,
        AVG(amount) OVER (PARTITION BY DATE_TRUNC('MONTH', paid_at) ORDER BY paid_at, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS amount_avg_5,
        LAST_VALUE(method, TRUE) OVER (PARTITION BY order_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_method
    FROM payments
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        status,
        order_id_stddev_samp_amount,
        SUM(amount_avg_5) OVER (PARTITION BY order_id ORDER BY order_id_stddev_samp_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount_avg_5,
        SUM(amount_avg_5) OVER (PARTITION BY status, order_id ORDER BY order_id_stddev_samp_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount_avg_5_2,
        AVG(order_id_stddev_samp_amount) OVER (PARTITION BY status) AS status_avg_order_id_stddev_samp_amount,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY order_id_stddev_samp_amount DESC, id) AS event_seq,
        LAG(order_id_stddev_samp_amount, 1) OVER (PARTITION BY status ORDER BY amount_avg_5, id) AS prev_order_id_stddev_samp_amount
    FROM metrics
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM with_windows
    INTERSECT
    SELECT id
    FROM payments
    WHERE order_id IS NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN matched_keys k ON s.id = k.id
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        status,
        status_avg_order_id_stddev_samp_amount,
        event_seq,
        INITCAP(TRIM(status)) AS status_title,
        UPPER(status) AS status_upper
    FROM kept_rows
    WHERE event_seq >= 6
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        status,
        event_seq,
        status_title,
        status_upper,
        INITCAP(TRIM(status)) AS status_title_2
    FROM cleaned
    WHERE status NOT IN ('SUCCESS', 'FAILED')
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        status,
        status_upper,
        status_title_2,
        CASE WHEN event_seq >= 7 THEN 'High' WHEN event_seq >= 4 THEN 'Medium' ELSE 'Low' END AS event_seq_band
    FROM filtered
    WHERE event_seq != 6
)
-- Aggregate with conditional metrics
SELECT
    status,
    SIZE(COLLECT_SET(status)) AS num_status,
    COUNT(DISTINCT id) AS distinct_ids,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) AS failed_count
FROM base
GROUP BY status;
