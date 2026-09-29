WITH with_windows AS (
    SELECT
        id,
        method,
        amount,
        paid_at,
        SUM(amount) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount,
        AVG(amount) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS amount_avg_5
    FROM payments
),
with_gaps AS (
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) > INTERVAL 14 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM with_windows
),
sessionized AS (
    SELECT
        id,
        method,
        amount,
        paid_at,
        amount_avg_5,
        SUM(new_group_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
enriched AS (
    SELECT
        id,
        method,
        amount_avg_5,
        LAG(session_id) OVER w_part AS prev_session_id,
        LAG(amount_avg_5) OVER w_part AS prev_amount_avg_5,
        LEAD(amount_avg_5) OVER w_part AS next_amount_avg_5,
        ROW_NUMBER() OVER w_part AS row_seq,
        AVG(amount) OVER (PARTITION BY YEAR(paid_at) ORDER BY amount_avg_5, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS amount_avg_2
    FROM sessionized
    WINDOW w_part AS (PARTITION BY method ORDER BY amount, id)
),
filtered AS (
    SELECT *, CAST(amount_avg_2 AS STRING) AS amount_avg_2_num
    FROM enriched
    WHERE method IS NOT NULL
      AND prev_session_id IS NOT NULL
),
scored AS (
    SELECT
        id,
        method,
        amount_avg_5,
        prev_session_id,
        prev_amount_avg_5,
        next_amount_avg_5,
        row_seq,
        amount_avg_2,
        amount_avg_2_num,
        NTILE(3) OVER (PARTITION BY method ORDER BY prev_session_id DESC, id) AS prev_session_id_bucket,
        NTILE(10) OVER (PARTITION BY method ORDER BY prev_session_id, id) AS prev_session_id_bucket_2,
        SUM(prev_amount_avg_5) OVER (PARTITION BY method ORDER BY prev_session_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev_amount_avg_5,
        LAST_VALUE(prev_session_id, TRUE) OVER (PARTITION BY method ORDER BY amount_avg_5, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev_session_id,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY row_seq, id) AS event_seq
    FROM filtered
)
SELECT
    CASE WHEN last_known_prev_session_id >= 8 THEN 'HIGH' ELSE 'LOW' END AS last_known_prev_session_id_tier,
    PERCENTILE_APPROX(next_amount_avg_5, 0.5) AS next_amount_avg_5_p50,
    COUNT(*) FILTER (WHERE method = 'bank_transfer') AS bank_transfer_rows,
    SIZE(COLLECT_SET(method)) AS num_methods
FROM scored
GROUP BY CASE WHEN last_known_prev_session_id >= 8 THEN 'HIGH' ELSE 'LOW' END
ORDER BY bank_transfer_rows DESC;
