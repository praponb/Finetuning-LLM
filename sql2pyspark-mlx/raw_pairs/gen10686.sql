WITH latest AS (
    SELECT
        id,
        amount,
        paid_at,
        status
    FROM payments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at DESC NULLS FIRST, id) = 1
),
filtered AS (
    SELECT
        *,
        ROUND(amount, 2) AS amount_rounded,
        CASE WHEN amount >= 1310 THEN 'High' WHEN amount >= 250 THEN 'Medium' ELSE 'Low' END AS amount_band
    FROM latest
    WHERE HOUR(paid_at) BETWEEN 6 AND 14
),
with_gaps AS (
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM filtered
),
islands AS (
    SELECT
        id,
        paid_at,
        status,
        amount_rounded,
        amount_band,
        SUM(is_new_burst) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
gaps AS (
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) > INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM islands
),
sessions_payments AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM gaps
),
latest_payments AS (
    SELECT
        id,
        paid_at,
        amount_rounded,
        amount_band,
        session_id
    FROM sessions_payments
    QUALIFY RANK() OVER (PARTITION BY status ORDER BY amount_rounded DESC NULLS LAST) = 1
)
SELECT
    DATE_TRUNC('QUARTER', paid_at) AS quarter_start,
    CASE WHEN session_id >= 8 THEN 'HIGH' ELSE 'LOW' END AS session_id_tier,
    MIN(paid_at) AS first_seen,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('QUARTER', paid_at) ORDER BY COUNT(DISTINCT id) DESC) AS distinct_ids_percent_rank
FROM latest_payments
GROUP BY DATE_TRUNC('QUARTER', paid_at), CASE WHEN session_id >= 8 THEN 'HIGH' ELSE 'LOW' END
HAVING COUNT(DISTINCT id) >= 1
ORDER BY quarter_start
