WITH valid AS (
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        status,
        COALESCE(amount, 0) AS amount_filled
    FROM payments
    WHERE paid_at >= TIMESTAMP '2023-04-26 19:00:00'
),
base AS (
    SELECT
        id,
        paid_at,
        status,
        amount_filled,
        COALESCE(amount, 0) AS amount_filled_2
    FROM valid
    WHERE paid_at > TO_DATE('2023-05-11')
),
valid_payments AS (
    SELECT *, ROUND(amount_filled_2 * 0.85, 2) AS amount_filled_2_adjusted
    FROM base
    WHERE amount_filled_2 <= 64
),
with_windows AS (
    SELECT
        *,
        SUM(amount_filled_2) OVER (PARTITION BY status ORDER BY amount_filled_2_adjusted, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount_filled_2,
        LAST_VALUE(amount_filled_2, TRUE) OVER (PARTITION BY status ORDER BY amount_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount_filled_2
    FROM valid_payments
),
totals AS (
    SELECT
        status,
        DATE_TRUNC('QUARTER', paid_at) AS quarter_start,
        CASE WHEN amount_filled_2 >= 78 THEN 'HIGH' ELSE 'LOW' END AS amount_filled_2_tier,
        SORT_ARRAY(COLLECT_SET(status)) AS status_list,
        ROUND(AVG(cumulative_amount_filled_2), 2) AS avg_cumulative_amount_filled_2,
        COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) AS success_count,
        (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS duration_min,
        ROUND(SUM(cumulative_amount_filled_2) / NULLIF(COUNT(CASE WHEN status = 'FAILED' THEN 1 END), 0), 2) AS cumulative_amount_filled_2_per_failed,
        RANK() OVER (PARTITION BY status ORDER BY ROUND(SUM(cumulative_amount_filled_2) / NULLIF(COUNT(CASE WHEN status = 'FAILED' THEN 1 END), 0), 2) DESC) AS cumulative_amount_filled_2_per_failed_rank
    FROM with_windows
    GROUP BY status, quarter_start, amount_filled_2_tier
)
SELECT
    status,
    quarter_start,
    amount_filled_2_tier,
    status_list,
    avg_cumulative_amount_filled_2,
    success_count,
    duration_min,
    cumulative_amount_filled_2_per_failed
FROM totals;
