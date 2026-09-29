WITH filtered AS (
    SELECT *, SUBSTRING(status, 1, 2) AS status_prefix
    FROM payments
    WHERE paid_at >= DATE '2023-09-23'
      AND method IS NOT DISTINCT FROM 'promptpay'
),
flagged AS (
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) > INTERVAL '7' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM filtered
),
sessions_payments AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
with_gaps AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM sessions_payments
),
sessions_payments_2 AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
cleaned AS (
    SELECT
        *,
        session_id - is_new_burst AS session_id_minus_is_new_burst,
        is_new_burst * amount AS is_new_burst_times_amount
    FROM sessions_payments_2
    WHERE paid_at > TO_DATE('2023-08-04')
      AND session_start_flag < 0
),
gaps AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag_2
    FROM cleaned
),
sessions_payments_3 AS (
    SELECT
        id,
        amount,
        paid_at,
        status,
        session_start_flag,
        island_id,
        is_new_burst,
        session_id,
        session_id_minus_is_new_burst,
        is_new_burst_times_amount,
        SUM(session_start_flag_2) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id_2
    FROM gaps
),
stats AS (
    SELECT
        CASE WHEN session_id_minus_is_new_burst >= 85 THEN 'PREMIUM' ELSE 'STANDARD' END AS session_id_minus_is_new_burst_tier,
        SUM(CASE WHEN status = 'SUCCESS' THEN amount ELSE 0 END) AS success_amount,
        COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) AS success_count,
        COUNT(DISTINCT id) AS distinct_ids,
        PERCENTILE_APPROX(session_start_flag, ARRAY(0.25, 0.5, 0.75)) AS session_start_flag_quartiles
    FROM sessions_payments_3
    GROUP BY 1
)
SELECT
    session_id_minus_is_new_burst_tier,
    success_amount,
    success_count,
    distinct_ids,
    session_start_flag_quartiles
FROM stats
ORDER BY distinct_ids
