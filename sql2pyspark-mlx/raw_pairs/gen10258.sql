WITH with_windows AS (
    SELECT
        id,
        status,
        ROW_NUMBER() OVER (PARTITION BY TO_DATE(paid_at), status ORDER BY paid_at DESC, id ASC) AS event_seq,
        ROUND(amount / SUM(amount) OVER (PARTITION BY DATE_TRUNC('WEEK', paid_at)), 4) AS amount_share
    FROM payments
),
metrics AS (
    SELECT
        id,
        event_seq,
        FIRST_VALUE(event_seq, TRUE) OVER (PARTITION BY status ORDER BY amount_share ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_seq,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY event_seq DESC) AS event_seq_dense_rank,
        AVG(amount_share) OVER (PARTITION BY status) AS status_avg_amount_share
    FROM with_windows
),
enriched AS (
    SELECT
        id,
        event_seq_dense_rank,
        status_avg_amount_share,
        NTILE(3) OVER (ORDER BY first_known_event_seq ASC, id ASC) AS first_known_event_seq_bucket,
        NTILE(3) OVER (ORDER BY event_seq_dense_rank DESC, id ASC) AS event_seq_dense_rank_bucket,
        SUM(event_seq_dense_rank) OVER (ORDER BY first_known_event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_event_seq_dense_rank,
        RANK() OVER (ORDER BY event_seq DESC NULLS FIRST) AS event_seq_rank,
        NTILE(5) OVER (ORDER BY event_seq_dense_rank ASC, id ASC) AS event_seq_dense_rank_bucket_2
    FROM metrics
),
metrics_payments AS (
    SELECT
        *,
        SUM(cumulative_event_seq_dense_rank) OVER (ORDER BY first_known_event_seq_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cumulative_event_seq_dense_rank,
        LAG(cumulative_event_seq_dense_rank, 1) OVER (ORDER BY event_seq_dense_rank ASC, id ASC) AS prev_cumulative_event_seq_dense_rank,
        NTILE(4) OVER w_running AS event_seq_rank_bucket,
        ROW_NUMBER() OVER (ORDER BY event_seq_dense_rank DESC, id ASC) AS event_seq
    FROM enriched
    WINDOW w_running AS (ORDER BY event_seq_rank ASC, id ASC)
)
SELECT
    CASE WHEN event_seq_dense_rank >= 8 THEN 'HIGH' ELSE 'LOW' END AS event_seq_dense_rank_tier,
    CASE WHEN event_seq_dense_rank_bucket >= 6 THEN 'LARGE' ELSE 'SMALL' END AS event_seq_dense_rank_bucket_tier,
    COUNT(*) AS event_count,
    APPROX_PERCENTILE(first_known_event_seq_bucket, 0.5) AS first_known_event_seq_bucket_p50,
    GROUPING_ID() AS grouping_level
FROM metrics_payments
GROUP BY GROUPING SETS ((CASE WHEN event_seq_dense_rank >= 8 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN event_seq_dense_rank_bucket >= 6 THEN 'LARGE' ELSE 'SMALL' END), (CASE WHEN event_seq_dense_rank >= 8 THEN 'HIGH' ELSE 'LOW' END))
ORDER BY first_known_event_seq_bucket_p50 ASC;
