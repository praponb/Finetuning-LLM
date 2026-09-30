WITH base AS (
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        UPPER(status) AS status_upper,
        MONTH(paid_at) AS paid_at_month,
        COALESCE(method, 'OTHER') AS method_filled
    FROM payments
    WHERE paid_at > TO_DATE('2023-06-03')
),
scored AS (
    SELECT
        *,
        ROW_NUMBER() OVER w_grp AS seq_no,
        LEAD(paid_at_month, 1, 0) OVER (PARTITION BY order_id ORDER BY amount ASC, id ASC) AS next_paid_at_month,
        STDDEV_SAMP(paid_at_month) OVER w_part AS order_id_stddev_samp_paid_at_month
    FROM base
    WINDOW w_grp AS (PARTITION BY order_id ORDER BY paid_at DESC, id ASC), w_part AS (PARTITION BY order_id)
),
flagged AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at ASC, id ASC) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at ASC, id ASC) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM scored
),
sessionized AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY order_id ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
stats AS (
    SELECT
        CASE WHEN seq_no >= 9 THEN 'LARGE' ELSE 'SMALL' END AS seq_no_tier,
        DATE_TRUNC('QUARTER', paid_at) AS quarter_start,
        PERCENTILE_APPROX(next_paid_at_month, ARRAY(0.25, 0.5, 0.75)) AS next_paid_at_month_quartiles,
        ROUND(AVG(seq_no), 2) AS avg_seq_no,
        ROUND(AVG(next_paid_at_month), 2) AS avg_next_paid_at_month,
        PERCENTILE_APPROX(order_id_stddev_samp_paid_at_month, 0.5) AS order_id_stddev_samp_paid_at_month_p50,
        (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS span_minutes
    FROM sessionized
    GROUP BY 1, 2
)
SELECT
    seq_no_tier,
    quarter_start,
    avg_seq_no,
    avg_next_paid_at_month,
    order_id_stddev_samp_paid_at_month_p50,
    span_minutes
FROM stats;
