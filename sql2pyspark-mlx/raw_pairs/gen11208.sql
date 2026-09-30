WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        status,
        DATE_FORMAT(paid_at, 'yyyy-MM') paid_at_label
    FROM payments
    WHERE paid_at >= DATE '2023-08-19'
      AND method IS NOT DISTINCT FROM 'promptpay'
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM cleaned
),
sessions_payments AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        status,
        SUM(is_new_session) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
joined AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.amount,
        a.paid_at,
        o.discount,
        o.amount AS order_amount,
        o.order_date
    FROM sessions_payments a
    LEFT JOIN orders o
        ON a.order_id = o.id
),
enriched AS (
    -- Attach orders attributes
    SELECT src.*, p.amount AS order_amount_2
    FROM joined src
    INNER JOIN orders p
        ON src.order_id = p.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(amount / SUM(amount) OVER (PARTITION BY order_id), 4) amount_share,
        AVG(order_amount) OVER w_running order_amount_avg_7d,
        NTILE(4) OVER w_ordered discount_bucket
    FROM enriched
    WINDOW w_running AS (PARTITION BY order_id ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW), w_ordered AS (ORDER BY discount, id)
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    order_id,
    kpi,
    metric_value
FROM (
    SELECT
        id,
        order_id,
        order_amount_2,
        amount_share,
        amount
    FROM metrics
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR kpi IN (order_amount_2 AS ORDER_AMOUNT_2, amount_share AS AMOUNT_SHARE, amount AS AMOUNT_VAL)
) unp
