WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM payments
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(burst_id) OVER (PARTITION BY order_id, status ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS burst_id_sum_7d,
        LAST_VALUE(method) IGNORE NULLS OVER (ORDER BY burst_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_method,
        SUM(burst_id) OVER (PARTITION BY order_id, method ORDER BY burst_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_burst_id
    FROM sessionized
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        new_group_flag,
        burst_id,
        burst_id_sum_7d,
        last_known_method,
        cumulative_burst_id,
        SUM(burst_id_sum_7d) OVER (PARTITION BY status ORDER BY cumulative_burst_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_burst_id_sum_7d,
        MIN(cumulative_burst_id) OVER w_recent AS status_min_cumulative_burst_id,
        SUM(new_group_flag) OVER w_recent AS status_sum_new_group_flag
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY status)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN status_sum_new_group_flag >= 3 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_sum_new_group_flag_tier,
        APPROX_PERCENTILE(burst_id, 0.9) AS burst_id_p90,
        SIZE(COLLECT_SET(status)) AS num_status,
        COUNT(*) AS num_rows
    FROM scored
    GROUP BY status_sum_new_group_flag_tier
)
SELECT status_sum_new_group_flag_tier
FROM summary
ORDER BY status_sum_new_group_flag_tier
