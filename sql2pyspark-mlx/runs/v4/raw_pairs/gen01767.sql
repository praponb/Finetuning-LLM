WITH valid AS (
    SELECT *, CONCAT_WS('-', plan, status) AS plan_status_key
    FROM subscriptions
    WHERE end_date >= TO_DATE('2024-05-30')
      AND monthly_fee IS NULL
),
with_dims AS (
    SELECT
        a.*,
        d.name,
        d.segment
    FROM valid a
    LEFT JOIN customers d
        ON a.customer_id = d.id
),
valid_subscriptions AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        plan_status_key,
        name,
        segment,
        DATE_TRUNC('month', start_date) AS start_date_period,
        COALESCE(segment, 'N/A') AS resolved_segment
    FROM with_dims
    WHERE customer_id IS NOT NULL
),
gaps AS (
    SELECT *, CASE WHEN LAG(start_date_period) OVER (PARTITION BY customer_id ORDER BY start_date_period, id) IS NULL
                 OR start_date_period > LAG(start_date_period) OVER (PARTITION BY customer_id ORDER BY start_date_period, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM valid_subscriptions
),
islands AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY customer_id ORDER BY start_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
SELECT
    id,
    plan,
    start_date,
    monthly_fee,
    status,
    plan_status_key,
    name,
    segment,
    start_date_period
FROM islands
ORDER BY status DESC NULLS FIRST
