WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TO_DATE(paid_at) AS paid_at_date,
        LENGTH(status) AS status_length,
        COALESCE(method, 'OTHER') AS method_clean
    FROM payments
    WHERE paid_at > TIMESTAMP '2023-08-02 07:00:00'
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        method,
        status,
        status_length,
        LAG(amount, 2) OVER (PARTITION BY order_id ORDER BY paid_at, id) AS prev_amount,
        NTILE(3) OVER w_running AS status_length_bucket,
        CUME_DIST() OVER (PARTITION BY MONTH(paid_at), status ORDER BY status_length DESC) AS status_length_cume_dist,
        SUM(status_length) OVER (PARTITION BY status ORDER BY status_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_status_length,
        STDDEV_SAMP(status_length) OVER (PARTITION BY status, order_id) AS status_stddev_samp_status_length
    FROM cleaned
    WINDOW w_running AS (PARTITION BY status ORDER BY status_length, id)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        method,
        status,
        status_length,
        prev_amount,
        status_length_bucket,
        status_length_cume_dist,
        running_status_length,
        MIN(status_stddev_samp_status_length) OVER (PARTITION BY status ORDER BY status_length, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_status_stddev_samp_status_length,
        COUNT(status_stddev_samp_status_length) OVER (PARTITION BY status ORDER BY status_length, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_status_stddev_samp_status_length,
        SUM(status_stddev_samp_status_length) OVER (PARTITION BY status ORDER BY status_length, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_status_stddev_samp_status_length,
        MAX(status_stddev_samp_status_length) OVER (PARTITION BY status ORDER BY status_length, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_status_stddev_samp_status_length
    FROM scored
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY last4_sum_status_stddev_samp_status_length DESC NULLS FIRST, id) AS latest_rank
    FROM with_windows
    QUALIFY latest_rank = 1
)
-- Join a dimension and aggregate
SELECT
    p.channel,
    p.status AS order_status,
    COUNT(*) AS cnt,
    MAX(a.last4_max_status_stddev_samp_status_length) AS max_last4_max_status_stddev_samp_status_length
FROM deduped a
INNER JOIN orders p ON a.order_id = p.id
GROUP BY ROLLUP (p.channel, p.status)
